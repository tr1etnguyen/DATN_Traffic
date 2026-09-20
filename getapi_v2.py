import requests
import pandas as pd
import time
from datetime import datetime, timezone
import os
import json

# ===========================
# 1. DANH SÁCH API KEYS
# ===========================
API_KEYS = [
    "cnxiHKTvclKQwSMpWtPoJjLZgJ3YNb2C",
    "BhvsX98HvjLMlDRFXLidX7nx0V2eChUc",
    "ncWSZE8vd6HyWudDai4UPx3rl5KDrU8F",
    "sm7UsMitEY9h5ARcP9pognJvm2C2kZHY",
    "o8qMdUxmXRQPZ70RZs9r1QWjt2D0hDOP",
    "Nqnl6ug5Rz8Eb5X5sdPTRdbVLWlyFrE5"
]

current_key_index = 0

INPUT_FILE = "tomtom_representative_points.csv"
OUTPUT_FILE = "cleaned.csv"

INTERVAL = 300  # 5 phút


# ===========================
# 2. KEY MANAGEMENT
# ===========================
def get_current_key():
    return API_KEYS[current_key_index]


def switch_key():
    global current_key_index

    current_key_index += 1

    # Nếu hết tất cả key
    if current_key_index >= len(API_KEYS):
        print("All API keys exhausted. Waiting for reset...")
        time.sleep(300)  # bạn có thể tăng lên 1h nếu cần
        current_key_index = 0

    print(f"Switched to API KEY #{current_key_index}")


# ===========================
# 3. CALL API (AUTO SWITCH KEY)
# ===========================
def get_traffic(lat, lon):
    url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

    while True:
        key = get_current_key()

        params = {
            "point": f"{lat},{lon}",
            "key": key
        }

        try:
            res = requests.get(url, params=params, timeout=10)

            # -----------------------
            # CASE 1: QUOTA HẾT
            # -----------------------
            if res.status_code in [403, 429]:
                print(f"Key exhausted: {key}")
                switch_key()
                continue  # retry cùng node

            if res.status_code != 200:
                print("HTTP Error:", res.status_code)
                return None

            data = res.json()

            # -----------------------
            # CASE 2: API báo lỗi trong JSON
            # -----------------------
            if "error" in data:
                print("API Error:", data["error"])
                switch_key()
                continue

            if "flowSegmentData" not in data:
                print("No flowSegmentData")
                return None

            return data

        except Exception as e:
            print("API error:", e)
            time.sleep(1)


# ===========================
# 4. FLATTEN JSON
# ===========================
def flatten_json(y, prefix=""):
    out = {}

    if isinstance(y, dict):
        for k, v in y.items():
            out.update(flatten_json(v, f"{prefix}{k}_"))
    elif isinstance(y, list):
        for i, v in enumerate(y):
            out.update(flatten_json(v, f"{prefix}{i}_"))
    else:
        out[prefix[:-1]] = y

    return out


# ===========================
# 5. LOAD NODES
# ===========================
df_nodes = pd.read_csv(INPUT_FILE)
print("Loaded nodes:", len(df_nodes))


# ===========================
# 6. COLLECT DATA (NO SKIP NODE)
# ===========================
def collect_once():
    records = []

    batch_time_iso = datetime.now(timezone.utc).isoformat()
    batch_time_unix = int(time.time())

    for _, row in df_nodes.iterrows():

        lat = row["representative_lat"]
        lon = row["representative_lon"]
        node_id = row["node_id"]

        # Retry cho đến khi lấy được data
        while True:
            data = get_traffic(lat, lon)

            if data:
                break

            print(f"Retry node {node_id}...")
            time.sleep(1)

        flat_data = flatten_json(data)

        # thêm metadata
        flat_data["timestamp"] = batch_time_iso
        flat_data["timestamp_unix"] = batch_time_unix
        # flat_data["node_id"] = node_id
        # flat_data["lat"] = lat
        # flat_data["lon"] = lon
        flat_data["raw_json"] = json.dumps(data)

        records.append(flat_data)

        print(f"[{batch_time_iso}] Collected node {node_id}")

        time.sleep(0.1)  # tránh spam API

    if not records:
        print("No data collected this round.")
        return

    df_new = pd.DataFrame(records)

    # append file
    if not os.path.exists(OUTPUT_FILE):
        df_new.to_csv(OUTPUT_FILE, index=False)
        print("Created new file:", OUTPUT_FILE)
    else:
        df_new.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
        print(f"Appended {len(df_new)} records")


# ===========================
# 7. MAIN LOOP
# ===========================
MAX_ROUNDS = 12  # 12 vòng = 1 giờ (5 phút mỗi vòng)

print("Start collecting traffic for 1 hour (12 rounds)...")

for round_idx in range(MAX_ROUNDS):
    print(f"\n================ ROUND {round_idx + 1}/{MAX_ROUNDS} ================")

    start_time = time.time()

    try:
        collect_once()
    except Exception as e:
        print("Error in collect:", e)

    # Nếu là vòng cuối thì không cần sleep nữa
    if round_idx == MAX_ROUNDS - 1:
        break

    elapsed = time.time() - start_time
    sleep_time = max(0, INTERVAL - elapsed)

    print(f"Sleeping {sleep_time:.2f} seconds before next round...\n")
    time.sleep(sleep_time)

print("\nFinished 1-hour data collection!")

#4/15