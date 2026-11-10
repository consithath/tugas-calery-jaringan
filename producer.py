"""Producer / input generator (padanan client.py pada UTS).

Membangkitkan request yang dikirim ke broker RabbitMQ lalu dikerjakan worker.

Contoh:
  python producer.py                      # 6 request, app selang-seling 1,2,1,2,...
  python producer.py -n 20 --pola acak    # 20 request, app acak
  python producer.py --app 1 --data hitung   # satu request saja
"""
import argparse
import random
import time
from collections import Counter

from tasks import APPS, long_task, short_task

TASKS = {1: long_task, 2: short_task}


def kirim(app_id: int, data: str):
    """Kirim satu request. Padanan validasi 'ERR' pada broker UTS."""
    if app_id not in TASKS:
        raise ValueError("ERR format harus: app_id 1|2")
    print(f"[Producer] Kirim : REQ {app_id} {data}")
    return TASKS[app_id].delay(data)


def bangkitkan(n: int, pola: str, seed):
    rng = random.Random(seed)
    for i in range(1, n + 1):
        app_id = (1 if i % 2 else 2) if pola == "selang-seling" else rng.choice([1, 2])
        yield app_id, f"req{i}"


def main():
    p = argparse.ArgumentParser(description="Producer / input generator Celery")
    p.add_argument("-n", "--jumlah", type=int, default=6, help="jumlah request")
    p.add_argument("--pola", choices=["selang-seling", "acak"], default="selang-seling")
    p.add_argument("--seed", type=int, default=None, help="seed untuk pola acak")
    p.add_argument("--app", type=int, help="kirim satu request dengan app_id ini")
    p.add_argument("--data", default="halo", help="data untuk --app")
    p.add_argument("--timeout", type=int, default=60)
    args = p.parse_args()

    if args.app is not None:
        jobs = [(args.app, args.data)]
    else:
        jobs = list(bangkitkan(args.jumlah, args.pola, args.seed))

    mulai = time.time()
    pending = []
    for app_id, data in jobs:
        try:
            pending.append((app_id, data, kirim(app_id, data)))
        except ValueError as e:
            print(f"[Producer] {e}")

    per_worker = Counter()
    for app_id, data, res in pending:
        out = res.get(timeout=args.timeout)
        per_worker[out["server_id"]] += 1
        print(f"[Producer] Balasan: RES {out['server_id']} {out['app_id']} {out['result']}")

    print(f"\n[Producer] Selesai dalam {time.time() - mulai:.1f} detik")
    print(f"[Producer] Total per worker: {dict(sorted(per_worker.items()))}")


if __name__ == "__main__":
    main()
