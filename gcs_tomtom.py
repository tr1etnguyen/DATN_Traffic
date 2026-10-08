import requests
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
import os
import json

# ============================================================
# GOOGLE CLOUD STORAGE
# ============================================================
from google.cloud import storage

# ============================================================
# TIMEZONE VIỆT NAM
# ============================================================

VIETNAM_TIMEZONE = timezone(
    timedelta(hours=7)
)
# ============================================================
# 1. DANH SÁCH API KEYS
# ============================================================

# Khuyến nghị:
# Không hard-code API key trong source code.
#
# Trên Linux VM:
#
# export TOMTOM_API_KEYS="cnxiHKTvclKQwSMpWtPoJjLZgJ3YNb2C,BhvsX98HvjLMlDRFXLidX7nx0V2eChUc,ncWSZE8vd6HyWudDai4UPx3rl5KDrU8F,sm7UsMitEY9h5ARcP9pognJvm2C2kZHY,o8qMdUxmXRQPZ70RZs9r1QWjt2D0hDOP,Nqnl6ug5Rz8Eb5X5sdPTRdbVLWlyFrE5"
#
# Sau đó Python sẽ tự đọc danh sách key.

API_KEYS = os.environ["TOMTOM_API_KEYS"].split(",")

current_key_index = 0


# ============================================================
# 2. FILE CẤU HÌNH
# ============================================================

# JSON có cấu trúc:
#
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
# 3. GOOGLE CLOUD STORAGE CONFIG
# ============================================================

# Thay bằng tên bucket của bạn
#
# Ví dụ:
#
# GCS_BUCKET_NAME = "traffic-dataset-bucket"

GCS_BUCKET_NAME = "tomtom_dataset"

# Thư mục dataset trên GCS
#
# Kết quả:
#
# gs://tomtom_dataset/traffic/
#     2026-10-08/
#         traffic_2026-10-08.csv
#
#     2026-10-09/
#         traffic_2026-10-09.csv

GCS_PREFIX = "traffic"


# ============================================================
# 4. KEY MANAGEMENT
# ============================================================

def get_current_key():

    return API_KEYS[current_key_index]


def switch_key():

    global current_key_index

    current_key_index += 1

    if current_key_index >= len(API_KEYS):

        print(
            "All API keys exhausted. "
            "Waiting 5 minutes before resetting..."
        )

        time.sleep(300)

        current_key_index = 0

    print(
        f"Switched to API KEY "
        f"#{current_key_index + 1}"
    )


# ============================================================
# 5. CALL TOMTOM TRAFFIC FLOW API
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

            res = requests.get(
                url,
                params=params,
                timeout=10
            )

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

                print(
                    "HTTP Error:",
                    res.status_code
                )

                return None

            data = res.json()

            # ------------------------------------------------
            # API trả lỗi bên trong JSON
            # ------------------------------------------------

            if "error" in data:

                print(
                    "API Error:",
                    data["error"]
                )

                switch_key()

                continue

            if "flowSegmentData" not in data:

                print(
                    "No flowSegmentData"
                )

                return None

            return data

        except requests.exceptions.RequestException as e:

            print(
                "Request error:",
                e
            )

            time.sleep(2)

        except json.JSONDecodeError as e:

            print(
                "JSON decode error:",
                e
            )

            return None


# ============================================================
# 6. FLATTEN JSON
# ============================================================

def flatten_json(value, prefix=""):

    out = {}

    if isinstance(value, dict):

        for key, item in value.items():

            out.update(
                flatten_json(
                    item,
                    f"{prefix}{key}_"
                )
            )

    elif isinstance(value, list):

        for index, item in enumerate(value):

            out.update(
                flatten_json(
                    item,
                    f"{prefix}{index}_"
                )
            )

    else:

        out[prefix[:-1]] = value

    return out


# ============================================================
# 7. LOAD NODES FROM JSON
# ============================================================

with open(
    INPUT_FILE,
    "r",
    encoding="utf-8"
) as f:

    network_data = json.load(f)


# Chỉ sử dụng trường "nodes".
#
# Trường "connections" hiện không được sử dụng.

nodes = network_data.get(
    "nodes",
    []
)


if not nodes:

    raise ValueError(
        "Không tìm thấy 'nodes' "
        "hoặc 'nodes' đang rỗng trong JSON."
    )


print(
    f"Loaded nodes from JSON: {len(nodes)}"
)


# ------------------------------------------------------------
# Kiểm tra cấu trúc node
# ------------------------------------------------------------

required_fields = {
    "name",
    "lat",
    "lon"
}


for index, node in enumerate(nodes):

    missing_fields = (
        required_fields - node.keys()
    )

    if missing_fields:

        raise ValueError(
            f"Node #{index} thiếu field: "
            f"{missing_fields}"
        )


# ------------------------------------------------------------
# Chuyển nodes thành DataFrame
# ------------------------------------------------------------

df_nodes = pd.DataFrame(
    nodes
)


print("Nodes:")

print(
    df_nodes[
        ["name", "lat", "lon"]
    ].to_string(index=False)
)


# ============================================================
# 8. GOOGLE CLOUD STORAGE FUNCTIONS
# ============================================================

def upload_daily_dataset(
    local_file,
    dataset_date
):
    """
    Upload dataset của một ngày lên GCS.

    Ví dụ:

    local_file:
        cleaned.csv

    dataset_date:
        2026-10-08

    GCS:

    gs://bucket/traffic/2026-10-08/
        traffic_2026-10-08.csv
    """

    print()
    print(
        "============================================================"
    )

    print(
        f"Uploading dataset for date: {dataset_date}"
    )

    print(
        f"Local file: {local_file}"
    )

    # --------------------------------------------------------
    # Kiểm tra file tồn tại
    # --------------------------------------------------------

    if not os.path.exists(local_file):

        print(
            f"File does not exist: {local_file}"
        )

        return False

    # --------------------------------------------------------
    # Kiểm tra file có dữ liệu hay không
    # --------------------------------------------------------

    if os.path.getsize(local_file) == 0:

        print(
            f"File is empty: {local_file}"
        )

        return False

    # --------------------------------------------------------
    # Tạo tên file trên GCS
    # --------------------------------------------------------

    destination_blob = (
        f"{GCS_PREFIX}/"
        f"{dataset_date}/"
        f"traffic_{dataset_date}.csv"
    )

    try:

        # ----------------------------------------------------
        # Tạo GCS client
        #
        # Google Cloud SDK sẽ tự sử dụng credentials
        # của VM nếu VM đã được cấp service account.
        # ----------------------------------------------------

        client = storage.Client()

        # ----------------------------------------------------
        # Lấy bucket
        # ----------------------------------------------------

        bucket = client.bucket(
            GCS_BUCKET_NAME
        )

        # ----------------------------------------------------
        # Tạo object/blob
        # ----------------------------------------------------

        blob = bucket.blob(
            destination_blob
        )

        # ----------------------------------------------------
        # Upload file
        # ----------------------------------------------------

        blob.upload_from_filename(
            local_file
        )

        print(
            "Upload successful!"
        )

        print(
            f"GCS path:"
        )

        print(
            f"gs://{GCS_BUCKET_NAME}/"
            f"{destination_blob}"
        )

        return True

    except Exception as e:

        print(
            "ERROR uploading dataset to GCS:"
        )

        print(e)

        return False


# ============================================================
# 9. COLLECT DATA FOR ONE ROUND
# ============================================================

def collect_once():

    records = []

    # ========================================================
    # THỜI GIAN VIỆT NAM
    # ========================================================

    batch_datetime = datetime.now(
        VIETNAM_TIMEZONE
    )

    batch_time_iso = (
        batch_datetime.isoformat()
    )

    # Unix timestamp vẫn là Unix timestamp chuẩn
    batch_time_unix = int(
        time.time()
    )

    for _, row in df_nodes.iterrows():

        name = row["name"]

        lat = float(
            row["lat"]
        )

        lon = float(
            row["lon"]
        )

        while True:

            data = get_traffic(
                lat,
                lon
            )

            if data is not None:
                break

            print(
                f"Retry node: {name}..."
            )

            time.sleep(2)

        flat_data = flatten_json(
            data
        )

        flat_data["name"] = name
        flat_data["lat"] = lat
        flat_data["lon"] = lon

        # Thời gian Việt Nam
        flat_data["timestamp"] = (
            batch_time_iso
        )

        # Unix timestamp
        flat_data["timestamp_unix"] = (
            batch_time_unix
        )

        flat_data["raw_json"] = json.dumps(
            data,
            ensure_ascii=False
        )

        records.append(
            flat_data
        )

        print(
            f"[{batch_time_iso}] "
            f"Collected: {name} | "
            f"lat={lat} | "
            f"lon={lon}"
        )

        time.sleep(0.1)

    if not records:

        print(
            "No data collected this round."
        )

        return

    df_new = pd.DataFrame(
        records
    )

    metadata_columns = [
        "name",
        "lat",
        "lon"
    ]

    other_columns = [
        column
        for column in df_new.columns
        if column not in metadata_columns
    ]

    df_new = df_new[
        metadata_columns + other_columns
    ]

    if not os.path.exists(
        OUTPUT_FILE
    ):

        df_new.to_csv(
            OUTPUT_FILE,
            index=False,
            encoding="utf-8-sig"
        )

        print(
            f"Created new file: "
            f"{OUTPUT_FILE} "
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
            f"Appended {len(df_new)} "
            f"records to {OUTPUT_FILE}"
        )


# ============================================================
# 10. MAIN LOOP - 24 HOURS
# ============================================================

print(
    f"Start collecting traffic for 24 hours "
    f"({MAX_ROUNDS} rounds, "
    f"every {INTERVAL // 60} minutes)..."
)


round_idx = 0


# ============================================================
# NGÀY DATASET HIỆN TẠI - GIỜ VIỆT NAM
# ============================================================

current_dataset_date = (
    datetime.now(
        VIETNAM_TIMEZONE
    ).date()
)

print(
    f"Current dataset date "
    f"(Vietnam): {current_dataset_date}"
)


# ============================================================
# MAIN LOOP
# ============================================================

while True:

    round_idx += 1

    print(
        f"\n================ "
        f"ROUND {round_idx} "
        f"================"
    )

    start_time = time.time()

    try:

        # ----------------------------------------------------
        # Lấy ngày hiện tại theo giờ Việt Nam
        # ----------------------------------------------------

        now_date = (
            datetime.now(
                VIETNAM_TIMEZONE
            ).date()
        )

        # ----------------------------------------------------
        # PHÁT HIỆN SANG NGÀY MỚI
        # ----------------------------------------------------

        if now_date != current_dataset_date:

            print()
            print(
                "============================================================"
            )

            print(
                "NEW VIETNAM DAY DETECTED!"
            )

            print(
                f"Previous dataset date: "
                f"{current_dataset_date}"
            )

            print(
                f"New dataset date: "
                f"{now_date}"
            )

            print(
                "============================================================"
            )

            # ------------------------------------------------
            # Upload dataset của ngày hôm trước
            # ------------------------------------------------

            upload_success = upload_daily_dataset(
                OUTPUT_FILE,
                current_dataset_date
            )

            # ------------------------------------------------
            # Chỉ xóa nếu upload thành công
            # ------------------------------------------------

            if upload_success:

                try:

                    os.remove(
                        OUTPUT_FILE
                    )

                    print(
                        f"Removed local file: "
                        f"{OUTPUT_FILE}"
                    )

                except OSError as e:

                    print(
                        "Could not remove local "
                        f"file: {e}"
                    )

            else:

                print(
                    "WARNING: Previous dataset "
                    "was NOT uploaded successfully."
                )

                print(
                    "Keeping local file."
                )

                pending_file = (
                    f"pending_"
                    f"{current_dataset_date}"
                    f".csv"
                )

                try:

                    os.rename(
                        OUTPUT_FILE,
                        pending_file
                    )

                    print(
                        f"Old dataset moved to: "
                        f"{pending_file}"
                    )

                except OSError as e:

                    print(
                        "Could not rename old "
                        f"dataset: {e}"
                    )

            # ------------------------------------------------
            # Bắt đầu ngày dataset mới
            # ------------------------------------------------

            current_dataset_date = (
                now_date
            )

            print(
                f"Started new dataset date: "
                f"{current_dataset_date}"
            )

        # ----------------------------------------------------
        # Thu thập dữ liệu
        # ----------------------------------------------------

        collect_once()

    except Exception as e:

        print(
            "Error in collect:",
            e
        )

    # --------------------------------------------------------
    # Không sleep sau vòng cuối
    # --------------------------------------------------------

    if round_idx == MAX_ROUNDS - 1:

        break

    elapsed = (
        time.time() - start_time
    )

    sleep_time = max(
        0,
        INTERVAL - elapsed
    )

    print(
        f"Round elapsed: "
        f"{elapsed:.2f}s"
    )

    print(
        f"Sleeping "
        f"{sleep_time:.2f}s "
        f"before next round..."
    )

    time.sleep(
        sleep_time
    )


# ============================================================
# 11. FINISHED
# ============================================================

print(
    "\nFinished 24-hour data collection!"
)