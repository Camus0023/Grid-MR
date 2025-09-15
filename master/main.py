from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional, List
from collections import Counter
import re, uuid, time, asyncio
import httpx

app = FastAPI(title="GridMR Master", version="0.5")

# ---------- Config de resiliencia ----------
MAX_RETRIES = 2               # reintentos por worker
REQUEST_TIMEOUT = 10.0        # segundos por request
RETRY_BACKOFF = 0.5           # backoff base (exponencial): 0.5s, 1s, 2s...
MAX_INFLIGHT = 16             # límite de MAPs simultáneos (throttle)

# Lista de workers (en Docker Compose se resuelven por nombre de servicio)
WORKERS: List[str] = [
    "http://worker1:8001",
    "http://worker2:8001",
]

# Estado por worker: fallas acumuladas y cooldown
WORKER_STATE: Dict[str, Dict[str, float]] = {
    w: {"failures": 0, "cooldown_until": 0.0} for w in WORKERS
}

def worker_available(w: str) -> bool:
    return time.time() >= WORKER_STATE.get(w, {}).get("cooldown_until", 0.0)

def mark_success(w: str) -> None:
    s = WORKER_STATE.setdefault(w, {"failures": 0, "cooldown_until": 0.0})
    s["failures"] = 0
    s["cooldown_until"] = 0.0

def mark_failure(w: str) -> None:
    s = WORKER_STATE.setdefault(w, {"failures": 0, "cooldown_until": 0.0})
    s["failures"] += 1
    cooldown = min(30.0, (2.0 ** s["failures"]))  # cooldown exponencial (cap 30s)
    s["cooldown_until"] = time.time() + cooldown

# ---------- Modelos ----------
class SubmitJob(BaseModel):
    job_id: Optional[str] = None
    input_text: str
    split_size: int = 5000

class JobStatus(BaseModel):
    job_id: str
    status: str                    # queued|running|done|error
    message: Optional[str] = None
    result: Optional[Dict[str, int]] = None
    elapsed_ms: Optional[int] = None
    total_splits: Optional[int] = None
    done_splits: Optional[int] = None

# ---------- Estado ----------
JOBS: Dict[str, JobStatus] = {}

# ---------- Utilidades (map/reduce local) ----------
_word_re = re.compile(r"[\wáéíóúñüÁÉÍÓÚÑÜ']+", re.UNICODE)

def local_map(text: str) -> Dict[str, int]:
    words = [w.lower() for w in _word_re.findall(text)]
    return dict(Counter(words))

def local_reduce(partials: List[Dict[str, int]]) -> Dict[str, int]:
    total = Counter()
    for p in partials:
        total.update(p)
    return dict(total)

def split_text(s: str, size: int) -> List[str]:
    if size <= 0:
        size = 5000
    words = _word_re.findall(s)
    chunks, curr, curr_len = [], [], 0
    for w in words:
        L = len(w) + 1
        if curr_len + L > size and curr:
            chunks.append(" ".join(curr))
            curr, curr_len = [w], L
        else:
            curr.append(w)
            curr_len += L
    if curr:
        chunks.append(" ".join(curr))
    return chunks

# ---------- HTTP helpers con retries ----------
async def post_with_retry(client: httpx.AsyncClient, url: str, payload: dict) -> httpx.Response:
    last_exc = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            await asyncio.sleep(RETRY_BACKOFF * (2 ** attempt))  # backoff exponencial
    raise last_exc

async def map_on_some_worker(
    client: httpx.AsyncClient, job_id: str, split_id: int, chunk: str, start_idx: int
) -> Dict[str, int]:
    """
    Intenta enviar este split a varios workers, saltando cooldowns y marcando fallas/éxitos.
    """
    n = len(WORKERS)
    tried = 0
    idx = start_idx % max(n, 1)
    payload = {"job_id": job_id, "split_id": split_id, "chunk": chunk}

    while tried < n and n > 0:
        w = WORKERS[idx]
        idx = (idx + 1) % n
        if not worker_available(w):
            tried += 1
            continue
        try:
            r = await post_with_retry(client, f"{w}/map", payload)
            mark_success(w)
            return r.json()["counts"]
        except Exception:
            mark_failure(w)
            tried += 1
            continue

    # si todos fallan para este split, el caller hace fallback local
    raise RuntimeError(f"no hay workers disponibles para el split {split_id}")

async def do_reduce_somewhere(client: httpx.AsyncClient, job_id: str, partials: List[Dict[str, int]]) -> Dict[str, int]:
    """
    Intenta hacer reduce en algún worker. Si todos fallan, reduce local.
    """
    n = len(WORKERS)
    if n == 0:
        return local_reduce(partials)

    for w in WORKERS:
        if not worker_available(w):
            continue
        try:
            r = await post_with_retry(client, f"{w}/reduce", {"job_id": job_id, "partials": partials})
            mark_success(w)
            return r.json()["counts"]
        except Exception:
            mark_failure(w)
            continue

    return local_reduce(partials)

async def preflight_health() -> None:
    """
    Ping a todos los workers para actualizar disponibilidad antes de empezar.
    """
    timeout = httpx.Timeout(2.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        tasks = []
        for w in WORKERS:
            tasks.append(client.get(f"{w}/"))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for w, r in zip(WORKERS, results):
            if isinstance(r, Exception):
                mark_failure(w)
            else:
                mark_success(w)

# ---------- Endpoints ----------
@app.get("/")
def root():
    return {
        "service": "GridMR Master (Distributed + Resilient + Throttled)",
        "version": "0.5",
        "workers": WORKERS,
        "worker_state": WORKER_STATE,
        "max_inflight": MAX_INFLIGHT,
    }

@app.post("/submit", response_model=JobStatus)
async def submit(job: SubmitJob):
    if not job.input_text or not job.input_text.strip():
        raise HTTPException(400, "input_text vacío")

    job_id = job.job_id or str(uuid.uuid4())
    JOBS[job_id] = JobStatus(job_id=job_id, status="queued")

    start = time.perf_counter()
    try:
        JOBS[job_id].status = "running"

        # Particiona y deja progreso inicial
        chunks = split_text(job.input_text, job.split_size)
        total = len(chunks)
        if total == 0:
            raise ValueError("No se generaron splits")
        JOBS[job_id].total_splits = total
        JOBS[job_id].done_splits = 0
        JOBS[job_id].message = f"0/{total} splits"

        # Preflight health para actualizar disponibilidad
        await preflight_health()

        # Si no hay workers, ejecuta local
        if not WORKERS:
            partials = [local_map(c) for c in chunks]
            final_counts = local_reduce(partials)
        else:
            limits = httpx.Limits(
                max_keepalive_connections=min(20, MAX_INFLIGHT),
                max_connections=MAX_INFLIGHT + 4,
            )
            timeout = httpx.Timeout(REQUEST_TIMEOUT)
            sem = asyncio.Semaphore(MAX_INFLIGHT)

            async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:

                async def map_or_local(idx: int, chunk: str) -> Dict[str, int]:
                    # throttle de concurrencia
                    async with sem:
                        try:
                            return await map_on_some_worker(client, job_id, idx, chunk, start_idx=idx)
                        except Exception:
                            # Fallback local si todos fallan para este split
                            return local_map(chunk)

                tasks = [asyncio.create_task(map_or_local(idx, chunk)) for idx, chunk in enumerate(chunks)]
                partials: List[Dict[str, int]] = []

                # Actualiza progreso a medida que terminan MAPs
                for coro in asyncio.as_completed(tasks):
                    res = await coro
                    partials.append(res)
                    st = JOBS[job_id]
                    st.done_splits = (st.done_splits or 0) + 1
                    st.elapsed_ms = int((time.perf_counter() - start) * 1000)
                    st.message = f"{st.done_splits}/{st.total_splits} splits"
                    JOBS[job_id] = st

                final_counts = await do_reduce_somewhere(client, job_id, partials)

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        JOBS[job_id] = JobStatus(
            job_id=job_id, status="done", result=final_counts,
            elapsed_ms=elapsed_ms, total_splits=total, done_splits=total
        )
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        st = JOBS[job_id]
        st.status = "error"
        st.message = str(e)
        st.elapsed_ms = elapsed_ms
        JOBS[job_id] = st

    return JOBS[job_id]

@app.get("/status/{job_id}", response_model=JobStatus)
def status(job_id: str):
    st = JOBS.get(job_id)
    if not st:
        raise HTTPException(404, "job no encontrado")
    return st
