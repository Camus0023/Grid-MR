# GridMR — MapReduce Distribuido (MVP)

Este repositorio contiene un **MVP funcional** de un sistema **MapReduce distribuido** con arquitectura **Maestro–Workers**.  
El **Master** expone una API para recibir trabajos (texto), los **particiona**, distribuye tareas **Map** a **dos Workers**, y realiza un **Reduce** final para devolver el resultado (conteo de palabras).

> Estado actual: **funciona en modo distribuido** con 1 Master + 2 Workers vía Docker Compose.  
> Próximo paso (pendiente): despliegue en **AWS/EC2** y mejoras de robustez/escala.

---

## 📁 Estructura del repositorio

```
gridmr_master/
├─ master/
│  └─ main.py                 # API del Master (distribuye Maps y Reduce final)
├─ worker/
│  └─ main.py                 # API del Worker (/map y /reduce)
├─ docker/
│  ├─ Dockerfile.master       # Imagen del Master
│  ├─ Dockerfile.worker       # Imagen del Worker (reutilizada por worker1/worker2)
│  └─ docker-compose.yml      # Orquestación 1 Master + 2 Workers
├─ requirements.txt           # Dependencias Python (común Master/Workers)
├─ .dockerignore
└─ .gitignore
```

---

## 🏗️ Arquitectura utilizada

### Estilo
- **Maestro–Workers**: El Master coordina; los Workers ejecutan cómputo.
- **Comunicación** sobre **HTTP/REST** (JSON) usando **FastAPI**.
- **Red**: En local, los contenedores comparten red de Docker y se resuelven por **nombre de servicio** (`worker1`, `worker2`).

### Diagrama (MVP)
```
Cliente           Master                 Worker1                  Worker2
   | POST /submit   |                        |                        |
   |---------------->|                        |                        |
   |                 | split(input)           |                        |
   |                 | map(chunk0) ---------->| POST /map             |
   |                 | map(chunk1) ---------------------------------->| POST /map
   |                 | ...                    |                        |
   |                 | <----------- parciales |                        |
   |                 | <----------------------| parciales              |
   |                 | reduce final (worker1) |                        |
   |      200 done   |                        |                        |
   |<----------------|                        |                        |
```

### Flujo lógico
1) Cliente → Master: `POST /submit` con `input_text` (y `split_size`).  
2) Master: tokeniza y **particiona sin cortar palabras**; asigna **Maps** con **round-robin** (worker1/worker2).  
3) Workers: **/map** devuelve parciales `{"palabra": conteo}`.  
4) Master: lanza **/reduce** final en **worker1** (MVP simple) y retorna el resultado al cliente.  

> **Modo de datos**: *push* (el texto se envía en JSON a los Workers).  
> **Estado**: en memoria (simple, volátil).  
> **Planificación**: round-robin (balanceo básico).

---

## 🧰 Tecnologías

- **Python 3.11**, **FastAPI**, **Uvicorn** (servicios HTTP).
- **httpx** (cliente HTTP asíncrono para Master → Workers).
- **Docker** + **Docker Compose** (orquestación local de 3 contenedores).
- **WSL2** (si corres en Windows 11) + **Docker Desktop**.

---

## 🚀 Cómo ejecutarlo (Docker recomendado)

### Requisitos
- Windows 11: **Docker Desktop** + **WSL2** activo.  
- Linux: **Docker Engine** + **Docker Compose plugin**.

### Pasos
```bash
git clone <URL_DEL_REPO>
cd gridmr_master/docker

# Construir imágenes
docker compose build

# Levantar servicios (Master + workers)
docker compose up -d

# Verifica contenedores y puertos
docker ps
# Esperado:
# gridmr_master  -> 0.0.0.0:8000->8000
# gridmr_worker1 -> 0.0.0.0:8001->8001
# gridmr_worker2 -> 0.0.0.0:8002->8001
```

### Health checks
```bash
# Workers
curl http://localhost:8001/
curl http://localhost:8002/

# Master
curl http://localhost:8000/
# Swagger UI:
# http://localhost:8000/docs
```

### Prueba básica (wordcount)
```bash
curl -s -X POST http://localhost:8000/submit \
  -H "Content-Type: application/json" \
  -d '{"input_text":"hola hola mundo mundo mundo","split_size":5000}'
# Resultado esperado (ejemplo):
# {"status":"done","result":{"hola":2,"mundo":3}, ...}
```

### Prueba de reparto (muchos splits)
1) Genera un texto grande y envíalo:
```bash
python3 - <<'PY'
with open('big.txt','w', encoding='utf-8') as f:
    f.write(('hola ' * 20000) + ('mundo ' * 30000))
print("OK big.txt generado")
PY

jq -Rs --argjson split 2000 '{input_text: ., split_size: $split}' big.txt \
| curl -s -X POST http://localhost:8000/submit \
  -H "Content-Type: application/json" -d @- \
| jq .
```
2) Revisa logs para ver **round-robin**:
```bash
# En el último minuto, cuántos /map atendió cada worker
docker logs --since 60s gridmr_worker1 | grep 'POST /map' | wc -l
docker logs --since 60s gridmr_worker2 | grep 'POST /map' | wc -l

# Reduce final (en este MVP ocurre en worker1)
docker logs --since 60s gridmr_worker1 | grep 'POST /reduce'
```

### Actualizar tras cambios de código
```bash
cd gridmr_master/docker
docker compose down
docker compose build --no-cache
docker compose up -d
```

### Apagar
```bash
docker compose down
```

---

## 🔌 APIs (contratos)

### Master
- `GET /` → Info y lista de workers configurados.
- `POST /submit`
  - **Request**:
    ```json
    {
      "job_id": "opcional-string",
      "input_text": "texto a procesar",
      "split_size": 5000
    }
    ```
  - **Response (200)**:
    ```json
    {
      "job_id": "uuid-o-el-que-enviaste",
      "status": "done|running|queued|error",
      "message": null,
      "result": { "palabra": 123 },
      "elapsed_ms": 12
    }
    ```
  - **Errores**: `400` si `input_text` está vacío.
- `GET /status/{job_id}` → Estado/resultado del job.  
  - **Errores**: `404` si no existe el `job_id`.

### Workers
- `GET /` → Info/health del worker.
- `POST /map`
  - **Request**:
    ```json
    { "job_id": "uuid", "split_id": 0, "chunk": "texto del split" }
    ```
  - **Response**:
    ```json
    { "worker": "worker1", "counts": { "hola": 2, "mundo": 1 } }
    ```
- `POST /reduce`
  - **Request**:
    ```json
    {
      "job_id": "uuid",
      "partials": [ { "hola": 2 }, { "hola": 1, "mundo": 3 } ]
    }
    ```
  - **Response**:
    ```json
    { "worker": "worker1", "counts": { "hola": 3, "mundo": 3 } }
    ```

---

## 🧪 Cómo demostrar que funciona (para el informe/video)

1) **docker ps** mostrando 3 contenedores.  
2) **Swagger** (`/docs`) mostrando endpoints.  
3) **POST /submit** con texto grande y `elapsed_ms` en la respuesta.  
4) **Logs**: conteo de llamadas `/map` por cada worker + `/reduce` en worker1.

---

## 🐞 Troubleshooting

- **“port is already allocated”** → cambia puertos en `docker-compose.yml` (p. ej., 8001→8011) o libera el puerto.  
- **Cambios no se reflejan** → siempre `compose down + build --no-cache + up -d`.  
- **Linux/Docker “permission denied”** → usa `sudo` o agrega tu usuario al grupo docker:
  ```bash
  sudo usermod -aG docker $USER && newgrp docker
  ```
- **pip “externally-managed-environment”** (modo local) → usa venv:
  ```bash
  python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
  ```

---

## 🧭 Qué falta / Próximos pasos

1) **Planificación mejorada**: timeouts y **reintentos**; **health-check/heartbeat** de workers; considerar **capacidad** (peso) por worker.  
2) **Shuffle/Reduce escalable**: múltiples reducers en paralelo (hash por clave: `hash(k) % R`).  
3) **Gestión de datos (modo “pull”)**: API de datos (tipo GridFS/S3) para que los workers **lean/escriban** en vez de enviar JSON.  
4) **Persistencia** de estado (jobs, parciales) en DB o caché (Redis/Postgres) y **resultados descargables**.  
5) **Seguridad**: autenticación del cliente hacia el Master y del Master hacia los Workers; HTTPS/Certs.  
6) **Observabilidad**: logs estructurados, métricas (Prometheus), trazas (OpenTelemetry).  
7) **CLI de cliente**: script o binario para enviar jobs y consultar resultados.  
8) **Despliegue en AWS**:
   - 1 EC2 **Master** (puerto 8000 público) y 2 EC2 **Workers** (puerto 8001 **solo** accesible desde el Master).
   - Docker en cada VM; Master apunta a **IPs privadas** de Workers.
   - Security Groups: limitar puertos de Workers al SG de Master.
   - Probar desde tu laptop `curl http://<IP_PUBLICA_MASTER>:8000/submit` y verificar tráfico interno a Workers.

---

## 📜 Licencia
MIT (o la que prefieras).
