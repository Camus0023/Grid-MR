from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import Dict, Optional, List, Tuple
from collections import Counter
import re, uuid, time, asyncio, os, shutil, json, sqlite3
import httpx
from pathlib import Path

app = FastAPI(title="GridMR Master", version="0.5")

# ---------------- Config ----------------
DATA_DIR = Path(os.getenv("MASTER_DATA_DIR", "/data/master"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "gridmr.db"

STATIC_WORKERS: List[str] = [
    os.getenv("WORKER1_URL", "http://worker1:8001"),
    os.getenv("WORKER2_URL", "http://worker2:8001"),
    os.getenv("WORKER3_URL", "http://worker3:8001"),
]

DEFAULT_NUM_REDUCERS = int(os.getenv("DEFAULT_NUM_REDUCERS", "2"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))   # seg (monitor)
HEARTBEAT_TTL = int(os.getenv("HEARTBEAT_TTL", "15"))            # seg (timeout)

# ---------------- Modelos ----------------
class WorkerInfo(BaseModel):
    url: str
    name: Optional[str] = None
    capacity: int = 1
    in_flight: int = 0                 # carga local estimada por el master
    healthy: bool = True
    last_error: Optional[str] = None
    last_seen: Optional[float] = None  # timestamp último heartbeat
    remote_in_flight: Optional[int] = None

class SubmitReq(BaseModel):
    input_text: str
    split_size: int = 5000
    num_reducers: int = DEFAULT_NUM_REDUCERS
    job_id: Optional[str] = None

class SubmitFileReq(BaseModel):
    job_id: str
    split_size: int = 1024
    num_reducers: int = DEFAULT_NUM_REDUCERS

class JobStatus(BaseModel):
    job_id: str
    status: str                # queued|running|done|error
    result: Optional[Dict[str,int]] = None
    message: Optional[str] = None
    elapsed_ms: Optional[int] = None
    map_attempts: Optional[int] = 0
    reducers: Optional[List[str]] = None

# ---------------- Estado ----------------
WORKERS: Dict[str, WorkerInfo] = {u: WorkerInfo(url=u) for u in STATIC_WORKERS}
JOBS: Dict[str, JobStatus] = {}

# ---------------- Persistencia (SQLite) ----------------
def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    with _db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
          job_id TEXT PRIMARY KEY,
          status TEXT NOT NULL,
          result_json TEXT,
          message TEXT,
          elapsed_ms INTEGER,
          map_attempts INTEGER,
          reducers_json TEXT,
          updated_at REAL NOT NULL
        );
        """)
        conn.commit()

def save_job(st: JobStatus):
    with _db() as conn:
        conn.execute("""
        INSERT INTO jobs(job_id,status,result_json,message,elapsed_ms,map_attempts,reducers_json,updated_at)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(job_id) DO UPDATE SET
          status=excluded.status,
          result_json=excluded.result_json,
          message=excluded.message,
          elapsed_ms=excluded.elapsed_ms,
          map_attempts=excluded.map_attempts,
          reducers_json=excluded.reducers_json,
          updated_at=excluded.updated_at;
        """, (
            st.job_id,
            st.status,
            json.dumps(st.result) if st.result is not None else None,
            st.message,
            st.elapsed_ms,
            st.map_attempts,
            json.dumps(st.reducers) if st.reducers is not None else None,
            time.time()
        ))
        conn.commit()

def load_job(job_id: str) -> Optional[JobStatus]:
    with _db() as conn:
        cur = conn.execute("SELECT job_id,status,result_json,message,elapsed_ms,map_attempts,reducers_json FROM jobs WHERE job_id=?;", (job_id,))
        row = cur.fetchone()
    if not row:
        return None
    result = json.loads(row[2]) if row[2] else None
    reducers = json.loads(row[6]) if row[6] else None
    return JobStatus(job_id=row[0], status=row[1], result=result, message=row[3], elapsed_ms=row[4], map_attempts=row[5], reducers=reducers)

# ---------------- Utilidades MR ----------------
_word_re = re.compile(r"[\wáéíóúñüÁÉÍÓÚÑÜ']+", re.UNICODE)

def local_reduce(partials: List[Dict[str, int]]) -> Dict[str, int]:
    total = Counter()
    for p in partials:
        total.update(p)
    return dict(total)

def split_text(s: str, chunk_size: int) -> List[str]:
    chunks = []
    i = 0
    while i < len(s):
        end = min(i + chunk_size, len(s))
        if end < len(s):
            j = s.rfind(" ", i, end)
            if j != -1 and j > i:
                end = j
        chunks.append(s[i:end])
        i = end
        if i < len(s) and s[i] == " ":
            i += 1
    return [c for c in chunks if c]

def split_file_text(fp: Path, approx_bytes: int) -> List[Tuple[int, Path]]:
    out_dir = fp.parent / "splits"
    out_dir.mkdir(parents=True, exist_ok=True)
    splits: List[Tuple[int, Path]] = []
    with fp.open("r", encoding="utf-8", errors="ignore") as f:
        buf, size, sid = [], 0, 0
        for line in f:
            lb = line.encode("utf-8")
            if size + len(lb) > approx_bytes and buf:
                p = out_dir / f"chunk_{sid}.txt"
                p.write_text("".join(buf), encoding="utf-8")
                splits.append((sid, p))
                buf, size, sid = [], 0, sid + 1
            buf.append(line); size += len(lb)
        if buf:
            p = out_dir / f"chunk_{sid}.txt"
            p.write_text("".join(buf), encoding="utf-8")
            splits.append((sid, p))
    return splits

def partition_by_key(partials: List[Dict[str,int]], R: int) -> List[Dict[str,int]]:
    shards = [Counter() for _ in range(max(1, R))]
    for p in partials:
        for k, v in p.items():
            shards[hash(k) % len(shards)][k] += v
    return [dict(c) for c in shards]

def choose_worker() -> WorkerInfo:
    healthy = [w for w in WORKERS.values() if w.healthy]
    if not healthy:
        healthy = list(WORKERS.values())
        if not healthy:
            raise HTTPException(503, "No hay workers registrados")
    return min(healthy, key=lambda w: (w.in_flight / max(1, w.capacity), w.in_flight))

def choose_n_workers(n: int) -> List[WorkerInfo]:
    ws = sorted(WORKERS.values(), key=lambda w: (0 if w.healthy else 1, w.in_flight / max(1, w.capacity), w.in_flight))
    picked, seen = [], set()
    for w in ws:
        if w.url not in seen:
            picked.append(w); seen.add(w.url)
        if len(picked) == n:
            break
    if not picked:
        raise HTTPException(503, "No hay workers registrados")
    while len(picked) < n:
        picked.append(picked[0])
    return picked

async def worker_upload(client: httpx.AsyncClient, worker: WorkerInfo, job_id: str, split_id: int, file_path: Path) -> str:
    files = {"file": (file_path.name, file_path.open("rb"), "text/plain")}
    data = {"job_id": job_id, "split_id": str(split_id)}
    r = await client.post(f"{worker.url}/upload", data=data, files=files, timeout=120)
    r.raise_for_status()
    return r.json()["file_path"]

async def worker_map(client: httpx.AsyncClient, worker: WorkerInfo, payload: dict) -> Dict[str,int]:
    try:
        worker.in_flight += 1
        r = await client.post(f"{worker.url}/map", json=payload, timeout=120)
        r.raise_for_status()
        data = r.json()
        worker.last_error = None
        worker.healthy = True
        return data["counts"]
    except Exception as e:
        worker.last_error = str(e); worker.healthy = False
        raise
    finally:
        worker.in_flight = max(0, worker.in_flight - 1)

async def worker_reduce(client: httpx.AsyncClient, worker: WorkerInfo, job_id: str, shard_partials: List[Dict[str,int]]) -> Dict[str,int]:
    try:
        worker.in_flight += 1
        payload = {"job_id": job_id, "partials": shard_partials}
        r = await client.post(f"{worker.url}/reduce", json=payload, timeout=180)
        r.raise_for_status()
        data = r.json()
        worker.last_error = None
        worker.healthy = True
        return data["counts"]
    except Exception as e:
        worker.last_error = str(e); worker.healthy = False
        raise
    finally:
        worker.in_flight = max(0, worker.in_flight - 1)

# ---------------- Endpoints ----------------
@app.on_event("startup")
async def _startup():
    init_db()
    # monitor de health (marca unhealthy si se pierde el heartbeat)
    async def monitor():
        while True:
            now = time.time()
            for w in WORKERS.values():
                if w.last_seen and (now - w.last_seen > HEARTBEAT_TTL):
                    w.healthy = False
            await asyncio.sleep(HEARTBEAT_INTERVAL)
    asyncio.create_task(monitor())

@app.get("/")
def root():
    return {"service": "GridMR Master", "version": app.version, "workers": [w.dict() for w in WORKERS.values()]}

@app.post("/register")
def register(url: str = Form(...), name: str = Form(None), capacity: int = Form(1)):
    wi = WORKERS.get(url) or WorkerInfo(url=url)
    wi.name = name or wi.name
    wi.capacity = max(1, int(capacity))
    wi.last_seen = time.time()
    wi.healthy = True
    WORKERS[url] = wi
    return {"registered": url, "count": len(WORKERS)}

@app.post("/heartbeat")
def heartbeat(url: str = Form(...), name: str = Form(None), capacity: int = Form(None), in_flight: int = Form(None)):
    wi = WORKERS.get(url) or WorkerInfo(url=url)
    if name: wi.name = name
    if capacity is not None: wi.capacity = max(1, int(capacity))
    if in_flight is not None: wi.remote_in_flight = int(in_flight)
    wi.last_seen = time.time()
    wi.healthy = True
    WORKERS[url] = wi
    return {"ok": True, "ts": wi.last_seen}

@app.get("/workers", response_model=List[WorkerInfo])
def list_workers():
    return list(WORKERS.values())

@app.post("/upload_job_input")
def upload_job_input(job_id: str = Form(...), file: UploadFile = File(...)):
    job_dir = DATA_DIR / "jobs" / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    dst = job_dir / "input.txt"
    with dst.open("wb") as out:
        shutil.copyfileobj(file.file, out)
    return {"job_id": job_id, "stored_at": str(dst)}

@app.post("/submit", response_model=JobStatus)
async def submit(req: SubmitReq):
    job_id = req.job_id or str(uuid.uuid4())
    st = JobStatus(job_id=job_id, status="running")
    JOBS[job_id] = st; save_job(st)
    start = time.perf_counter()
    attempts = 0
    try:
        chunks = split_text(req.input_text, req.split_size)
        if not chunks:
            st = JobStatus(job_id=job_id, status="done", result={}, elapsed_ms=0, reducers=[])
            JOBS[job_id] = st; save_job(st); return st

        async with httpx.AsyncClient() as client:
            # MAP
            map_tasks = []
            for i, chunk in enumerate(chunks):
                attempts += 1
                for attempt in range(2):
                    worker = choose_worker()
                    try:
                        payload = {"job_id": job_id, "split_id": i, "chunk": chunk}
                        map_tasks.append(worker_map(client, worker, payload)); break
                    except Exception:
                        if attempt == 1: raise
            map_partials = await asyncio.gather(*map_tasks)

            # SHUFFLE/PARTITION
            R = max(1, req.num_reducers)
            partitions = partition_by_key(map_partials, R)

            # REDUCE (distribuido)
            reducers = choose_n_workers(R)
            red_tasks = [worker_reduce(client, reducers[i], job_id, [partitions[i]]) for i in range(R)]
            reduced_shards = await asyncio.gather(*red_tasks)

        final_counts = local_reduce(reduced_shards)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        st = JobStatus(job_id=job_id, status="done", result=final_counts, elapsed_ms=elapsed_ms, map_attempts=attempts, reducers=[w.url for w in reducers])
        JOBS[job_id] = st; save_job(st); return st
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        st = JobStatus(job_id=job_id, status="error", message=str(e), elapsed_ms=elapsed_ms, map_attempts=attempts)
        JOBS[job_id] = st; save_job(st); return st

@app.post("/submit_file", response_model=JobStatus)
async def submit_file(req: SubmitFileReq):
    job_id = req.job_id
    in_path = DATA_DIR / "jobs" / job_id / "input.txt"
    if not in_path.exists():
        raise HTTPException(400, f"No hay input para job_id={job_id}. Sube el archivo a /upload_job_input primero.")
    st = JobStatus(job_id=job_id, status="running")
    JOBS[job_id] = st; save_job(st)
    start = time.perf_counter()
    attempts = 0
    try:
        splits = split_file_text(in_path, req.split_size)
        if not splits:
            st = JobStatus(job_id=job_id, status="done", result={}, elapsed_ms=0, reducers=[])
            JOBS[job_id] = st; save_job(st); return st

        async with httpx.AsyncClient() as client:
            # MAP (upload + map por file_path)
            map_tasks = []
            for sid, chunk_path in splits:
                attempts += 1
                for attempt in range(2):
                    worker = choose_worker()
                    try:
                        remote_fp = await worker_upload(client, worker, job_id, sid, chunk_path)
                        payload = {"job_id": job_id, "split_id": sid, "file_path": remote_fp}
                        map_tasks.append(worker_map(client, worker, payload)); break
                    except Exception:
                        if attempt == 1: raise
            map_partials = await asyncio.gather(*map_tasks)

            # SHUFFLE/PARTITION
            R = max(1, req.num_reducers)
            partitions = partition_by_key(map_partials, R)

            # REDUCE (distribuido)
            reducers = choose_n_workers(R)
            red_tasks = [worker_reduce(client, reducers[i], job_id, [partitions[i]]) for i in range(R)]
            reduced_shards = await asyncio.gather(*red_tasks)

        final_counts = local_reduce(reduced_shards)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        st = JobStatus(job_id=job_id, status="done", result=final_counts, elapsed_ms=elapsed_ms, map_attempts=attempts, reducers=[w.url for w in reducers])
        JOBS[job_id] = st; save_job(st); return st
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        st = JobStatus(job_id=job_id, status="error", message=str(e), elapsed_ms=elapsed_ms, map_attempts=attempts)
        JOBS[job_id] = st; save_job(st); return st

@app.get("/status/{job_id}", response_model=JobStatus)
def status(job_id: str):
    st = JOBS.get(job_id)
    if st:
        return st
    loaded = load_job(job_id)
    if not loaded:
        raise HTTPException(404, "job no encontrado")
    return loaded