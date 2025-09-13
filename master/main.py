from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional, List
from collections import Counter
import re, uuid, time, asyncio
import httpx

app = FastAPI(title="GridMR Master", version="0.2")

# Lista de workers (en Docker Compose se resuelven por nombre de servicio)
WORKERS: List[str] = [
    "http://worker1:8001",
    "http://worker2:8001",
]

# --- Modelos ---
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

# --- Estado ---
JOBS: Dict[str, JobStatus] = {}

# --- Utilidades (map/reduce local) ---
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

# --- Endpoints ---
@app.get("/")
def root():
    return {"service": "GridMR Master (Distributed-ready)", "version": "0.2", "workers": WORKERS}

@app.post("/submit", response_model=JobStatus)
async def submit(job: SubmitJob):
    if not job.input_text or not job.input_text.strip():
        raise HTTPException(400, "input_text vacío")

    job_id = job.job_id or str(uuid.uuid4())
    JOBS[job_id] = JobStatus(job_id=job_id, status="queued")

    start = time.perf_counter()
    try:
        JOBS[job_id].status = "running"
        chunks = split_text(job.input_text, job.split_size)
        if not chunks:
            raise ValueError("No se generaron splits")

        # Si no hay workers definidos, ejecuta local (fallback)
        if not WORKERS:
            partials = [local_map(c) for c in chunks]
            final_counts = local_reduce(partials)
        else:
            # Distribuido: enviar maps a workers round-robin y reducir en worker0
            async with httpx.AsyncClient(timeout=60) as client:
                tasks = []
                for idx, chunk in enumerate(chunks):
                    w = WORKERS[idx % len(WORKERS)]
                    tasks.append(
                        client.post(f"{w}/map", json={"job_id": job_id, "split_id": idx, "chunk": chunk})
                    )
                responses = await asyncio.gather(*tasks)
                partials = [r.json()["counts"] for r in responses]

                # Reduce final en worker 0 (simple)
                r = await client.post(f"{WORKERS[0]}/reduce", json={"job_id": job_id, "partials": partials})
                final_counts = r.json()["counts"]

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        JOBS[job_id] = JobStatus(job_id=job_id, status="done", result=final_counts, elapsed_ms=elapsed_ms)
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        JOBS[job_id] = JobStatus(job_id=job_id, status="error", message=str(e), elapsed_ms=elapsed_ms)

    return JOBS[job_id]

@app.get("/status/{job_id}", response_model=JobStatus)
def status(job_id: str):
    st = JOBS.get(job_id)
    if not st:
        raise HTTPException(404, "job no encontrado")
    return st
