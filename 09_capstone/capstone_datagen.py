# Databricks notebook source
# MAGIC %md
# MAGIC # Capstone data generator: StreamFit
# MAGIC Writes telemetry, clickstream and user CDC files into a volume. Run `emit(minutes=5)` repeatedly (or in a loop)
# MAGIC while your streams are running to simulate live traffic.

# COMMAND ----------

import json, os, random, csv, uuid
from datetime import datetime, timedelta

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "streamfit")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
for d in ["telemetry", "clickstream", "users", "devices"]:
    os.makedirs(f"{ROOT}/{d}", exist_ok=True)

N_USERS = 2000
rng = random.Random(7)
REGIONS = ["NA", "EU", "APAC", "LATAM"]
PAGES = ["home", "workouts", "workout_detail", "profile", "settings", "upgrade", "community"]
STATE = {"clock": datetime(2026, 3, 1, 6, 0, 0), "batch": 0}

# COMMAND ----------

def write_users_and_devices(day: int):
    """Day 1 = all users on 'free'. Later days: some upgrade/downgrade/change email; a few new users."""
    rows = []
    ids = range(1, N_USERS + 1) if day == 1 else rng.sample(range(1, N_USERS + 1), 80)
    for uid in ids:
        r = random.Random(uid)
        rows.append({"user_id": uid, "email": f"user{uid}@streamfit.example", "birthdate": f"{r.randint(1960, 2006)}-0{r.randint(1, 9)}-1{r.randint(0, 9)}",
                     "region": r.choice(REGIONS), "plan": "free" if day == 1 else rng.choice(["free", "premium", "premium"]),
                     "op": "INSERT" if day == 1 else "UPDATE", "op_ts": str(datetime(2026, 3, day, 0, 0) + timedelta(seconds=uid))})
    with open(f"{ROOT}/users/users_{day:03d}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
    if day == 1:
        with open(f"{ROOT}/devices/devices.csv", "w", newline="") as fh:
            w = csv.writer(fh); w.writerow(["device_id", "user_id", "model", "firmware"])
            for uid in range(1, N_USERS + 1):
                w.writerow([f"D{uid:05d}", uid, random.Random(uid).choice(["Pulse S", "Pulse X"]), "1.4"])


def emit(minutes: int = 5, active_users: int = 300):
    """Emit `minutes` of telemetry + clickstream, with duplicates, late events and (after batch 3) firmware v2."""
    STATE["batch"] += 1
    b, start = STATE["batch"], STATE["clock"]
    tel, clicks = [], []
    for uid in rng.sample(range(1, N_USERS + 1), active_users):
        for s in range(0, minutes * 60, 10):
            ts = start + timedelta(seconds=s + rng.randint(0, 9))
            e = {"event_id": str(uuid.uuid4()), "device_id": f"D{uid:05d}", "event_ts": ts.isoformat(),
                 "heart_rate": rng.randint(55, 185), "steps": rng.randint(0, 40), "battery_pct": max(1, 100 - b * 2 - rng.randint(0, 30))}
            if b > 3:
                e["firmware"] = "2.0"; e["spo2"] = rng.randint(90, 100)     # schema evolution
            if rng.random() < 0.02:
                e["event_ts"] = (ts - timedelta(minutes=rng.randint(5, 45))).isoformat()  # late event
            tel.append(e)
            if rng.random() < 0.01:
                tel.append(dict(e))                                         # duplicate send
        t = start
        for _ in range(rng.randint(1, 8)):
            t += timedelta(seconds=rng.randint(5, 400))
            page = rng.choice(PAGES)
            clicks.append({"event_id": str(uuid.uuid4()), "user_id": uid, "event_ts": t.isoformat(),
                           "event_type": rng.choice(["page_view", "page_view", "workout_started", "workout_finished"])
                                         if page.startswith("workout") else "page_view",
                           "page": page, "app_version": rng.choice(["5.1.0", "5.2.0"])})
    rng.shuffle(tel)
    for name, rows in [("telemetry", tel), ("clickstream", clicks)]:
        with open(f"{ROOT}/{name}/{name}_{b:05d}.json", "w") as fh:
            fh.write("\n".join(json.dumps(r) for r in rows))
    STATE["clock"] = start + timedelta(minutes=minutes)
    print(f"batch {b}: {len(tel)} telemetry, {len(clicks)} clicks, clock={STATE['clock']}")

# COMMAND ----------

write_users_and_devices(1)
for _ in range(3):
    emit()

# COMMAND ----------

# MAGIC %md Simulate later days of CRM changes: `write_users_and_devices(2)`, `write_users_and_devices(3)`, …
# MAGIC Or keep traffic flowing:
# MAGIC ```python
# MAGIC import time
# MAGIC for _ in range(20): emit(); time.sleep(15)
# MAGIC ```
