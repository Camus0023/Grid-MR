from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, List
from collections import Counter
import os, re

app = FastAPI(title="GridMR Worker", version="0.1")
WORKER_NAME = os.getenv("WORKER_NAME", "worker")

_word_re = re.compile(r"[\wáéíóúñüÁÉÍÓÚÑÜ']+", re.UNICODE)

class MapReq(BaseModel):
    job_id: str
    split_id: int
    chunk: str

class ReduceReq(BaseModel):
    job_id: str
    partials: List[Dict[str, int]]

@app.get("/")
def root():
    return {"service": "GridMR Worker", "name": WORKER_NAME}

@app.post("/map")
def do_map(req: MapReq):
    words = [w.lower() for w in _word_re.findall(req.chunk)]
    counts = dict(Counter(words))
    return {"worker": WORKER_NAME, "counts": counts}

@app.post("/reduce")
def do_reduce(req: ReduceReq):
    total = Counter()
    for p in req.partials:
        total.update(p)
    return {"worker": WORKER_NAME, "counts": dict(total)}
