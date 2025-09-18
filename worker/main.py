from fastapi import FastAPI, UploadFile, File, Form
from pydantic import BaseModel
from typing import Dict, List, Optional
from collections import Counter
import os, re, asyncio
from pathlib import Path
import httpx

app = FastAPI(title="GridMR Worker", version="0.4")

WORKER_NAME = os.getenv("WORKER_NAME", "worker")
CAPACITY = int(os.getenv("CAPACITY", "1"))
DATA_DIR = Path(os.getenv("WORKER_DATA_DIR", "/data/worker")); DATA_DIR.mkdir(parents=True, exist_ok=True)

MASTER_URL = os.getenv("MASTER_URL", "http://master:8000")
PUBLIC_URL = os.getenv("PUBLIC_URL")  # ej: http://worker1:8001
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))

IN_FLIGHT = 0
_word_re = re.compile(r"[\wáéíóúñüÁÉÍÓÚÑÜ']+", re.UNICODE)

class MapReq(BaseModel):
    job_id: str
    split_id: int
    chunk: Optional[str] = None
    file_path: Optional[str] = None

class ReduceReq(BaseModel):
    job_id: str
    partials: List[Dict[str,int]]

@app.get("/")
def info():
    return {"service": "GridMR Worker", "name": WORKER_NAME, "capacity": CAPACITY, "data_dir": str(DATA_DIR)}

@app.post("/upload")
def upload(job_id: str = Form(...), split_id: str = Form(...), file: UploadFile = File(...)):
    job_dir = DATA_DIR / "jobs" / job_id / "incoming"; job_dir.mkdir(parents=True, exist_ok=True)
    dst = job_dir / f"split_{split_id}__{file.filename}"
    with dst.open("wb") as out: out.write(file.file.read())
    return {"file_path": str(dst)}

@app.post("/map")
def do_map(req: MapReq):
    global IN_FLIGHT; IN_FLIGHT += 1
    try:
        if req.file_path:
            text = Path(req.file_path).read_text(encoding="utf-8", errors="ignore")
        else:
            text = req.chunk or ""
        words = [w.lower() for w in _word_re.findall(text)]
        return {"worker": WORKER_NAME, "counts": dict(Counter(words))}
    finally:
        IN_FLIGHT = max(0, IN_FLIGHT - 1)

@app.post("/reduce")
def do_reduce(req: ReduceReq):
    total = Counter()
    for p in req.partials: total.update(p)
    return {"worker": WORKER_NAME, "counts": dict(total)}

@app.on_event("startup")
async def startup_register_and_heartbeat():
    public_url = PUBLIC_URL
    if not public_url:
        # Fallback (mejor definir PUBLIC_URL por env)
        host = os.environ.get("HOSTNAME", WORKER_NAME)
        public_url = f"http://{host}:8001"

    async def register_once():
        for _ in range(12):
            try:
                async with httpx.AsyncClient() as client:
                    data = {"url": public_url, "name": WORKER_NAME, "capacity": str(CAPACITY)}
                    r = await client.post(f"{MASTER_URL}/register", data=data, timeout=5)
                    r.raise_for_status(); return
            except Exception:
                await asyncio.sleep(2)

    async def heartbeat_loop():
        while True:
            try:
                async with httpx.AsyncClient() as client:
                    data = {"url": public_url, "name": WORKER_NAME, "capacity": str(CAPACITY), "in_flight": str(IN_FLIGHT)}
                    await client.post(f"{MASTER_URL}/heartbeat", data=data, timeout=5)
            except Exception:
                pass
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    await register_once()
    asyncio.create_task(heartbeat_loop())