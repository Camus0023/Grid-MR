from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional
from collections import Counter
import re
import uuid
import time

app = FastAPI(title="GridMR Master (Local MVP)", version="0.1")

# --- Modelos ---
class SubmitJob(BaseModel):
    job_id: Optional[str] = None   # si no viene, se genera
    input_text: str
    split_size: int = 5000         # tamaño de split en caracteres (simple)

class JobStatus(BaseModel):
    job_id: str
    status: str                    # queued|running|done|error
    message: Optional[str] = None
    result: Optional[Dict[str, int]] = None
    elapsed_ms: Optional[int] = None

# --- Estado en memoria ---
JOBS: Dict[str, JobStatus] = {}

# --- Utilidades (map/reduce local) ---
_word_re = re.compile(r"[\wáéíóúñüÁÉÍÓÚÑÜ']+", re.UNICODE)

def local_map(text: str) -> Dict[str, int]:
    words = [w.lower() for w in _word_re.findall(text)]
    return dict(Counter(words))

def local_reduce(partials: list[Dict[str, int]]) -> Dict[str, int]:
    total = Counter()
    for p in partials:
        total.update(p)
    return dict(total)

def split_text(s: str, size: int) -> list[str]:
    """
    Particiona respetando palabras (no corta dentro de una palabra).
    Usa el mismo patrón de palabras del map para extraer tokens y luego
    arma chunks que no superen 'size' aprox. en caracteres.
    """
    if size <= 0:
        size = 5000
    words = _word_re.findall(s)
    chunks = []
    curr = []
    curr_len = 0

    for w in words:
        L = len(w) + 1  # +1 por el espacio al unir
        if curr_len + L > size and curr:
            chunks.append(" ".join(curr))
            curr = [w]
            curr_len = L
        else:
            curr.append(w)
            curr_len += L

    if curr:
        chunks.append(" ".join(curr))
    return chunks


# --- Endpoints ---
@app.get("/")
def root():
    return {"service": "GridMR Master (Local)", "version": "0.1"}

@app.post("/submit", response_model=JobStatus)
def submit(job: SubmitJob):
    if not job.input_text or not job.input_text.strip():
        raise HTTPException(400, "input_text vacío")

    job_id = job.job_id or str(uuid.uuid4())
    JOBS[job_id] = JobStatus(job_id=job_id, status="queued")

    # Ejecutar localmente de forma síncrona (MVP)
    start = time.perf_counter()
    try:
        JOBS[job_id].status = "running"
        chunks = split_text(job.input_text, job.split_size)
        if not chunks:
            raise ValueError("No se generaron splits")

        partials = [local_map(chunk) for chunk in chunks]
        final_counts = local_reduce(partials)
        elapsed_ms = int((time.perf_counter() - start) * 1000)

        JOBS[job_id] = JobStatus(
            job_id=job_id, status="done", result=final_counts, elapsed_ms=elapsed_ms
        )
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        JOBS[job_id] = JobStatus(
            job_id=job_id, status="error", message=str(e), elapsed_ms=elapsed_ms
        )

    return JOBS[job_id]

@app.get("/status/{job_id}", response_model=JobStatus)
def status(job_id: str):
    st = JOBS.get(job_id)
    if not st:
        raise HTTPException(404, "job no encontrado")
    return st
