# GridMR — MapReduce Distribuido (MVP)

Sistema de procesamiento distribuido tipo **MapReduce** con arquitectura **Maestro–Workers** y comunicación **REST** (FastAPI). Este MVP ejecuta **WordCount** sobre texto enviado por el cliente, partiendo la entrada en *splits* y distribuyendo **Map** entre múltiples workers; el **Reduce** se intenta ejecutar en un worker disponible (con *fallback* local).

---

## 🗂️ Estructura del repositorio

```
Grid-MR-feat-master-mvp/
├─ master/
│  └─ main.py                 # API Master: split, scheduler con resiliencia, map distribuido y reduce (fallback local)
├─ worker/
│  └─ main.py                 # API Worker: /map y /reduce (WordCount)
├─ docker/
│  ├─ Dockerfile.master       # Imagen del Master
│  ├─ Dockerfile.worker       # Imagen del Worker
│  ├─ docker-compose.yml      # Orquestación local (1 Master + 2 Workers)
│  └─ big.txt                 # Texto grande de prueba
├─ requirements.txt           # Dependencias comunes (FastAPI, httpx, pydantic, ...)
├─ .dockerignore
└─ .gitignore
```

---

## 🧭 Arquitectura y flujo

**Estilo**: Maestro–Workers (HTTP/REST + JSON).
**Resolución de servicios**: en Compose, el Master alcanza a los Workers por **nombre** (`worker1`, `worker2`).

**Diagrama (MVP)**

```
Cliente           Master                            Workers
   |  POST /submit   |                                   |
   |---------------->| split(input)                      |
   |                 |  map(split0)  ────────────────▶  /map
   |                 |  map(split1)  ─────────────────▶  /map
   |                 |  ...                             ...
   |                 |  reduce(parciales) ────────────▶  /reduce
   |      200 done   | (fallback reduce local si falla) |
   ◀-----------------┘                                   |
```

**Planificación y resiliencia**

* **Pre‑flight health**: el Master *pingea* `GET /` de cada worker al iniciar un job.
* **Reintentos**: `MAX_RETRIES=2` por split y por request.
* **Cooldown exponencial** por worker tras fallas (cap 30s); los workers en cooldown se **saltan** temporalmente.
* **Throttling**: `MAX_INFLIGHT=16` limita la cantidad de MAP concurrentes.
* **Fallback local**: si un split no logra ejecutarse en ningún worker, el Master aplica `local_map` para ese split.
* **Reduce**: se intenta en el **primer worker disponible**; si todos fallan, se hace **reduce local** en el Master.

> Estado del job en memoria (`JOBS`): `queued|running|done|error`, progreso de splits y `elapsed_ms`.

---

## ⚙️ Requisitos

* **Docker** y **Docker Compose**.
* Windows 11: **Docker Desktop** + **WSL2** activo.
* Linux/macOS: Docker Engine + plugin de compose.

---

## 🚀 Puesta en marcha (local con Docker)

Desde `docker/`:

```bash
# 1) Construir imágenes
docker compose build

# 2) Levantar servicios
docker compose up -d

# 3) Ver contenedores y puertos
docker ps
# Esperado:
# gridmr_master  -> 0.0.0.0:8000->8000
# gridmr_worker1 -> 0.0.0.0:8001->8001
# gridmr_worker2 -> 0.0.0.0:8002->8001
```

**Health checks**

```bash
# Master
curl http://localhost:8000/
# Swagger UI: http://localhost:8000/docs

# Workers
curl http://localhost:8001/
curl http://localhost:8002/
```

---

## 🧪 Pruebas rápidas

### A) WordCount (texto en crudo)

**Linux/macOS**

```bash
curl -s -X POST http://localhost:8000/submit \
  -H "Content-Type: application/json" \
  -d '{"input_text":"hola mundo hola gridmr map reduce","split_size":1024}' | jq .
```

**Windows PowerShell**

```powershell
$body = @{ input_text = "hola mundo hola gridmr map reduce"; split_size = 1024 } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8000/submit" -Method Post -Body $body -ContentType "application/json"
```

### B) Carga mayor (muchos splits)

```bash
python3 - <<'PY'
with open('big.txt','w', encoding='utf-8') as f:
    f.write(('hola ' * 20000) + ('mundo ' * 30000))
print('OK big.txt')
PY

jq -Rs --argjson split 2000 '{input_text: ., split_size: $split}' docker/big.txt \
| curl -s -X POST http://localhost:8000/submit \
  -H "Content-Type: application/json" -d @- \
| jq .
```

> Observa en los logs cómo el Master reparte los MAP y aplica **reintentos/cooldown** si un worker falla.

---

## 🔌 API de referencia (MVP)

### Master

* `GET /` → Info del servicio.
* `POST /submit` (JSON) → Ejecuta MR sobre `input_text`.

  * **Request**

    ```json
    {
      "job_id": "opcional",
      "input_text": "texto a procesar",
      "split_size": 1024
    }
    ```
  * **Response (200)**

    ```json
    {
      "job_id": "uuid-o-el-que-enviaste",
      "status": "done|running|error",
      "message": "n/m splits",
      "result": { "palabra": 123 },
      "elapsed_ms": 12,
      "total_splits": 10,
      "done_splits": 10
    }
    ```
* `GET /status/{job_id}` → Estado/resultado del job (en memoria).

### Worker

* `GET /` → Info/health del worker.
* `POST /map`

  * **Request**

    ```json
    { "job_id": "uuid", "split_id": 0, "chunk": "texto del split" }
    ```
  * **Response**

    ```json
    { "worker": "worker1", "counts": { "hola": 2, "mundo": 1 } }
    ```
* `POST /reduce`

  * **Request**

    ```json
    { "job_id": "uuid", "partials": [ { "hola": 2 }, { "hola": 1, "mundo": 3 } ] }
    ```
  * **Response**

    ```json
    { "worker": "worker1", "counts": { "hola": 3, "mundo": 3 } }
    ```

---

## 🔧 Configuración

### Variables de entorno

* **Master**

  * `WORKERS` → lista separada por comas con URLs base de workers. Ej.: `http://worker1:8001,http://worker2:8001`
    (si se omite, usa esos dos valores por defecto).
* **Worker**

  * `WORKER_NAME` → etiqueta amigable del worker.

### Parámetros de resiliencia (codificados en el Master)

* `MAX_RETRIES=2`, `REQUEST_TIMEOUT=10s`, `RETRY_BACKOFF=0.5s` (exponencial), `MAX_INFLIGHT=16`.

> Se pueden ajustar editando constantes en `master/main.py`.

---

## 🐞 Troubleshooting

* **“port is already allocated”** → cambia puertos en `docker/docker-compose.yml` o libera el puerto.
* **Cambios no se reflejan** → `docker compose down && docker compose build --no-cache && docker compose up -d`.
* **Windows PowerShell** → usa `Invoke-RestMethod` en lugar de `curl -H -d` (o fuerza `curl.exe`).
* **Workers caídos** → el Master hará reintentos y aplicará **cooldown**; revisa `docker logs gridmr_worker*`.

---

## ☁️ Despliegue en AWS/EC2

### Infra
- 1x EC2 Ubuntu 22.04 (master), 3x EC2 (workers) `t3.micro`, 20 GiB gp3.
- Security Group `gridmr-sg`:
  - Inbound: SSH 22 → My IP; TCP 8000 → My IP; TCP 8000 → VPC CIDR; TCP 8001 → VPC CIDR.
  - Outbound: All traffic (0.0.0.0/0).
- Master con puerto 8000 público; workers solo trafican dentro de la VPC (8001).

### Master (SSH)
```bash
sudo apt update && sudo apt install -y docker.io docker-compose git
git clone -b Palacio --single-branch https://github.com/Camus0023/Grid-MR.git gridmr
cd gridmr && sudo mkdir -p /srv/gridmr/master && sudo chown -R ubuntu:ubuntu /srv/gridmr/master
cd docker
docker-compose -f docker-compose.master.yml up -d --build
# (opcional) mapear workers en /etc/hosts si el master muestra placeholders
sudo bash -c 'cat >> /etc/hosts <<EOF
172.31.X.1 worker1
172.31.X.2 worker2
172.31.X.3 worker3
EOF'
curl http://localhost:8000/workers
```
### Worker (SSH en cada uno; cambia WORKER_NAME y usa la IP privada del master)
```bash
sudo apt update && sudo apt install -y docker.io docker-compose git
git clone -b Palacio --single-branch https://github.com/Camus0023/Grid-MR.git gridmr
sudo mkdir -p /srv/gridmr/worker && sudo chown -R ubuntu:ubuntu /srv/gridmr/worker
cd gridmr/docker
MASTER_PRIV_IP="172.31.24.235"           # IP privada del master
WORKER_NAME="worker1"                     # worker2 / worker3 en los otros
WORKER_PRIV_IP=$(hostname -I | awk '{print $1}')
cat > docker-compose.yml <<EOF
services:
  worker:
    build:
      context: ../
      dockerfile: docker/Dockerfile.worker
    container_name: gridmr_${WORKER_NAME}
    ports: ["8001:8001"]
    environment:
      - WORKER_NAME=${WORKER_NAME}
      - MASTER_URL=http://${MASTER_PRIV_IP}:8000
      - PUBLIC_URL=http://${WORKER_PRIV_IP}:8001
      - CAPACITY=1
      - HEARTBEAT_INTERVAL=5
    volumes: ["/srv/gridmr/worker:/data"]
    restart: unless-stopped
EOF
docker-compose up -d --build
```
### Prueba desde pc (Cliente)
``` bash
# Ver workers
curl http://<IP_PUBLICA_MASTER>:8000/workers

# Texto
python client.py --master http://<IP_PUBLICA_MASTER>:8000 submit-text "hola hola mundo mundo mundo" --split 1024 --reducers 2

# Archivo
python client.py --master http://<IP_PUBLICA_MASTER>:8000 submit-file --job wordcount sample.txt --split 2048 --reducers 2

# Status
python client.py --master http://<IP_PUBLICA_MASTER>:8000 status <job_id|wordcount>
```
### Evidencia para la entrega
<img width="1458" height="217" alt="image" src="https://github.com/user-attachments/assets/07c115a4-4ad6-4e64-a0a0-00a754d6f24b" />
En esta imagen podemos ver que desde una terminal en un pc se ven los 3 workers,

<img width="1150" height="381" alt="image" src="https://github.com/user-attachments/assets/2e90e5c0-5f80-415d-96b5-ffbde5722023" />
En esta imagen se puede ver un job enviado y finalizado con exito.
