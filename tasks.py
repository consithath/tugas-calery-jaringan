"""Definisi task: padanan aplikasi Long dan Short pada worker UTS."""
import time

from celery_app import app

APPS = {1: "LONG", 2: "SHORT"}


def _server_id(task) -> str:
    """Nama worker yang melayani (padanan server-id pada UTS), mis. 'w1@laptop'."""
    return task.request.hostname.split("@")[0]


@app.task(bind=True, name="tasks.long_task")
def long_task(self, data: str) -> dict:
    """Application ID 1 - perhitungan kompleks (disimulasikan jeda 3 detik)."""
    server_id = _server_id(self)
    print(f"[{server_id}] Terima request app=LONG data='{data}'")
    time.sleep(3)
    result = f"hasil komputasi kompleks untuk '{data}'"
    print(f"[{server_id}] Selesai app=LONG data='{data}'")
    return {"server_id": server_id, "app_id": 1, "result": result}


@app.task(bind=True, name="tasks.short_task")
def short_task(self, data: str) -> dict:
    """Application ID 2 - echo / perhitungan sederhana."""
    server_id = _server_id(self)
    print(f"[{server_id}] Terima request app=SHORT data='{data}'")
    result = f"echo: {data}"
    print(f"[{server_id}] Selesai app=SHORT data='{data}'")
    return {"server_id": server_id, "app_id": 2, "result": result}
