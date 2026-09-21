import requests
import pandas as pd
import time
from datetime import datetime, timezone
import os
import json

# ============================================================
# 1. DANH SÁCH API KEYS
# ============================================================
# Khuyến nghị: không hard-code API key trong source code.
# Có thể thay bằng biến môi trường TOMTOM_API_KEYS.
API_KEYS = [
    "cnxiHKTvclKQwSMpWtPoJjLZgJ3YNb2C",
    "BhvsX98HvjLMlDRFXLidX7nx0V2eChUc",
    "ncWSZE8vd6HyWudDai4UPx3rl5KDrU8F",
    "sm7UsMitEY9h5ARcP9pognJvm2C2kZHY",
    "o8qMdUxmXRQPZ70RZs9r1QWjt2D0hDOP",
    "Nqnl6ug5Rz8Eb5X5sdPTRdbVLWlyFrE5"
]

current_key_index = 0

# ============================================================
# 2. FILE CẤU HÌNH
# ============================================================
# JSON có cấu trúc:
# {
#   "nodes": [
#       {"name": "...", "lat": ..., "lon": ...}
#   ],
#   "connections": [...]
# }
INPUT_FILE = "target.json"

OUTPUT_FILE = "cleaned.csv"

# Lấy dữ liệu mỗi 5 phút
INTERVAL = 300

# 24 giờ / 5 phút = 288 vòng
MAX_ROUNDS = 24 * 60 * 60 // INTERVAL


# ============================================================
# 3. KEY MANAGEMENT
# ============================================================
def get_current_key():
    return API_KEYS[current_key_index]


def switch_key():
    global current_key_index

    current_key_index += 1

    if current_key_index >= len(API_KEYS):
        print("All API keys exhausted. Waiting 5 minutes before resetting...")
        time.sleep(300)
        current_key_index = 0

    print(f"Switched to API KEY #{current_key_index + 1}")


# ============================================================
# 4. CALL TOMTOM TRAFFIC FLOW API
# ============================================================
def get_traffic(lat, lon):
    url = (
        "https://api.tomtom.com/traffic/services/4/"
        "flowSegmentData/absolute/10/json"
    )

    while True:
        key = get_current_key()

        params = {
            "point": f"{lat},{lon}",
            "key": key
        }

        try:
            res = requests.get(url, params=params, timeout=10)

            # ------------------------------------------------
            # API key hết quota / bị từ chối
            # ------------------------------------------------
            if res.status_code in [403, 429]:
                print(
                    f"Key #{current_key_index + 1} exhausted "
                    f"(HTTP {res.status_code})"
                )
                switch_key()
                continue

            # ------------------------------------------------
            # HTTP error khác
            # ------------------------------------------------
            if res.status_code != 200:
                print("HTTP Error:", res.status_code)
                return None

            data = res.json()

            # ------------------------------------------------
            # API trả lỗi bên trong JSON
            # ------------------------------------------------
            if "error" in data:
                print("API Error:", data["error"])
                switch_key()
                continue

            if "flowSegmentData" not in data:
                print("No flowSegmentData")
                return None

            return data

        except requests.exceptions.RequestException as e:
            print("Request error:", e)
            time.sleep(2)

        except json.JSONDecodeError as e:
            print("JSON decode error:", e)
            return None


# ============================================================
# 5. FLATTEN JSON
# ============================================================
def flatten_json(value, prefix=""):
    out = {}

    if isinstance(value, dict):
        for key, item in value.items():
            out.update(flatten_json(item, f"{prefix}{key}_"))

    elif isinstance(value, list):
        for index, item in enumerate(value):
            out.update(flatten_json(item, f"{prefix}{index}_"))

    else:
        out[prefix[:-1]] = value

    return out


# ============================================================
# 6. LOAD NODES FROM JSON
# ============================================================
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    network_data = json.load(f)

# Chỉ sử dụng trường "nodes".
# Trường "connections" hiện không được sử dụng.
nodes = network_data.get("nodes", [])

if not nodes:
    raise ValueError("Không tìm thấy 'nodes' hoặc 'nodes' đang rỗng trong JSON.")

print(f"Loaded nodes from JSON: {len(nodes)}")

# Kiểm tra cấu trúc node
required_fields = {"name", "lat", "lon"}

for index, node in enumerate(nodes):
    missing_fields = required_fields - node.keys()

    if missing_fields:
        raise ValueError(
            f"Node #{index} thiếu field: {missing_fields}"
        )

# Chuyển nodes thành DataFrame để dễ xử lý
df_nodes = pd.DataFrame(nodes)

print("Nodes:")
print(df_nodes[["name", "lat", "lon"]].to_string(index=False))


# ============================================================
# 7. COLLECT DATA FOR ONE ROUND
# ============================================================
def collect_once():
    records = []

    # Thời điểm bắt đầu lấy một batch
    batch_time_iso = datetime.now(timezone.utc).isoformat()
    batch_time_unix = int(time.time())

    for _, row in df_nodes.iterrows():

        # ----------------------------------------------------
        # Lấy trực tiếp từ JSON:
        # name -> tên node / tên đường / nút giao
        # lat  -> latitude
        # lon  -> longitude
        # ----------------------------------------------------
        name = row["name"]
        lat = float(row["lat"])
        lon = float(row["lon"])

        # ----------------------------------------------------
        # Retry cho đến khi lấy được traffic data
        # ----------------------------------------------------
        while True:
            data = get_traffic(lat, lon)

            if data is not None:
                break

            print(f"Retry node: {name}...")
            time.sleep(2)

        # ----------------------------------------------------
        # Flatten dữ liệu TomTom
        # ----------------------------------------------------
        flat_data = flatten_json(data)

        # ----------------------------------------------------
        # Metadata của node
        # ----------------------------------------------------
        # Quan trọng:
        # Thêm name vào cùng record với dữ liệu TomTom,
        # giúp biết traffic data thuộc node nào.
        flat_data["name"] = name
        flat_data["lat"] = lat
        flat_data["lon"] = lon

        # Thời gian thu thập
        flat_data["timestamp"] = batch_time_iso
        flat_data["timestamp_unix"] = batch_time_unix

        # Lưu nguyên JSON TomTom nếu cần kiểm tra/debug
        flat_data["raw_json"] = json.dumps(
            data,
            ensure_ascii=False
        )

        records.append(flat_data)

        print(
            f"[{batch_time_iso}] "
            f"Collected: {name} | "
            f"lat={lat} | lon={lon}"
        )

        # Tránh gửi request quá dồn
        time.sleep(0.1)

    if not records:
        print("No data collected this round.")
        return

    df_new = pd.DataFrame(records)

    metadata_columns = [
        "name",
        "lat",
        "lon"
    ]


    # Các cột còn lại
    other_columns = [
        column
        for column in df_new.columns
        if column not in metadata_columns
    ]


    # Đưa name, lat, lon lên đầu
    df_new = df_new[
        metadata_columns + other_columns
    ]


    # --------------------------------------------------------
    # Append vào CSV
    # --------------------------------------------------------
    if not os.path.exists(OUTPUT_FILE):
        df_new.to_csv(
            OUTPUT_FILE,
            index=False,
            encoding="utf-8-sig"
        )

        print(
            f"Created new file: {OUTPUT_FILE} "
            f"({len(df_new)} records)"
        )

    else:
        df_new.to_csv(
            OUTPUT_FILE,
            mode="a",
            header=False,
            index=False,
            encoding="utf-8-sig"
        )

        print(
            f"Appended {len(df_new)} records to {OUTPUT_FILE}"
        )


# ============================================================
# 8. MAIN LOOP - 24 HOURS
# ============================================================
print(
    f"Start collecting traffic for 24 hours "
    f"({MAX_ROUNDS} rounds, every {INTERVAL // 60} minutes)..."
)
round_idx = 0
#for round_idx in range(MAX_ROUNDS):
while True:
    round_idx += 1
    print(
        f"\n================ "
        f"ROUND {round_idx} "
        f"================"
    )

    start_time = time.time()

    try:
        collect_once()

    except Exception as e:
        print("Error in collect:", e)

    # Không sleep sau vòng cuối
    if round_idx == MAX_ROUNDS - 1:
        break

    # --------------------------------------------------------
    # Đảm bảo thời điểm bắt đầu round tiếp theo cách nhau
    # khoảng INTERVAL giây.
    # --------------------------------------------------------
    elapsed = time.time() - start_time

    sleep_time = max(
        0,
        INTERVAL - elapsed
    )

    print(
        f"Round elapsed: {elapsed:.2f}s"
    )

    print(
        f"Sleeping {sleep_time:.2f}s "
        f"before next round..."
    )

    time.sleep(sleep_time)


print("\nFinished 24-hour data collection!")
