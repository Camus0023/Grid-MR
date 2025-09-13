import argparse, json, sys, pathlib, requests

def post(url, data=None, json_=None, files=None):
    r = requests.post(url, data=data, json=json_, files=files, timeout=120)
    r.raise_for_status()
    return r.json()

def get(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    ap = argparse.ArgumentParser("GridMR client")
    ap.add_argument("--master", required=True, help="URL del master, p.ej http://localhost:8000")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("submit-text")
    s1.add_argument("--split", type=int, default=1024)
    s1.add_argument("--reducers", type=int, default=2)
    s1.add_argument("text", help="Texto a procesar")

    s2 = sub.add_parser("submit-file")
    s2.add_argument("--job", required=True)
    s2.add_argument("--split", type=int, default=64_000, help="~bytes por chunk")
    s2.add_argument("--reducers", type=int, default=2)
    s2.add_argument("path", help="Archivo de entrada (texto)")

    s3 = sub.add_parser("status")
    s3.add_argument("job_id")

    args = ap.parse_args()

    if args.cmd == "submit-text":
        payload = {"input_text": args.text, "split_size": args.split, "num_reducers": args.reducers}
        print(json.dumps(post(f"{args.master}/submit", json_=payload), indent=2, ensure_ascii=False))

    elif args.cmd == "submit-file":
        p = pathlib.Path(args.path)
        if not p.exists(): sys.exit(f"No existe: {p}")
        print("[1/2] Subiendo archivo…")
        res = post(f"{args.master}/upload_job_input", data={"job_id": args.job}, files={"file": (p.name, p.open("rb"), "text/plain")})
        print("[2/2] Lanzando job…")
        payload = {"job_id": args.job, "split_size": args.split, "num_reducers": args.reducers}
        print(json.dumps(post(f"{args.master}/submit_file", json_=payload), indent=2, ensure_ascii=False))

    elif args.cmd == "status":
        print(json.dumps(get(f"{args.master}/status/{args.job_id}"), indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()