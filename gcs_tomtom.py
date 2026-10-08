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

# Không hard-code API key trong source code.
#
# Trên Linux VM:
#
# export TOMTOM_API_KEYS="cnxiHKTvclKQwSMpWtPoJjLZgJ3YNb2C,BhvsX98HvjLMlDRFXLidX7nx0V2eChUc,ncWSZE8vd6HyWudDai4UPx3rl5KDrU8F,sm7UsMitEY9h5ARcP9pognJvm2C2kZHY,o8qMdUxmXRQPZ70RZs9r1QWjt2D0hDOP,Nqnl6ug5Rz8Eb5X5sdPTRdbVLWlyFrE5"
#
# Sau đó Python sẽ tự đọc danh sách key.

API_KEYS = [
    key.strip()
    for key in os.environ["TOMTOM_API_KEYS"].split(",")
    if key.strip()
]

if not API_KEYS:
    raise ValueError(
        "TOMTOM_API_KEYS is empty."
    )


current_key_index = 0


# ============================================================
# 2. FILE CẤU HÌNH
# ============================================================

INPUT_FILE = "target.json"

OUTPUT_FILE = "cleaned.csv"

# Lấy dữ liệu mỗi 5 phút
INTERVAL = 300

# 24 giờ / 5 phút = 288 round
#
# MAX_ROUNDS KHÔNG dùng để dừng chương trình.
#
# Nó chỉ biểu thị số round tối đa trong một ngày
# nếu collector chạy liên tục đủ 24 giờ.
MAX_ROUNDS = (
    24 * 60 * 60
) // INTERVAL


# ============================================================
# 3. GOOGLE CLOUD STORAGE CONFIG
# ============================================================

GCS_BUCKET_NAME = "tomtom_dataset"

GCS_PREFIX = "traffic"


# ============================================================
# 4. KEY MANAGEMENT
# ============================================================

def get_current_key():

    return API_KEYS[
        current_key_index
    ]


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
                    f"Key #{current_key_index + 1} "
                    f"exhausted "
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

            # ------------------------------------------------
            # Kiểm tra dữ liệu
            # ------------------------------------------------

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

def flatten_json(
    value,
    prefix=""
):

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


# ------------------------------------------------------------
# Chỉ sử dụng trường "nodes"
# ------------------------------------------------------------

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
    f"Loaded nodes from JSON: "
    f"{len(nodes)}"
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
        f"Uploading dataset for date: "
        f"{dataset_date}"
    )

    print(
        f"Local file: "
        f"{local_file}"
    )


    # --------------------------------------------------------
    # Kiểm tra file tồn tại
    # --------------------------------------------------------

    if not os.path.exists(local_file):

        print(
            f"File does not exist: "
            f"{local_file}"
        )

        return False


    # --------------------------------------------------------
    # Kiểm tra file có dữ liệu
    # --------------------------------------------------------

    if os.path.getsize(local_file) == 0:

        print(
            f"File is empty: "
            f"{local_file}"
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
        # ----------------------------------------------------

        client = storage.Client()


        # ----------------------------------------------------
        # Lấy bucket
        # ----------------------------------------------------

        bucket = client.bucket(
            GCS_BUCKET_NAME
        )


        # ----------------------------------------------------
        # Tạo blob
        # ----------------------------------------------------

        blob = bucket.blob(
            destination_blob
        )


        # ----------------------------------------------------
        # Upload
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


    # Unix timestamp chuẩn
    batch_time_unix = int(
        time.time()
    )


    # ========================================================
    # COLLECT TỪNG NODE
    # ========================================================

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
                f"Retry node: "
                f"{name}..."
            )

            time.sleep(2)


        # ----------------------------------------------------
        # Flatten JSON
        # ----------------------------------------------------

        flat_data = flatten_json(
            data
        )


        # ----------------------------------------------------
        # Metadata
        # ----------------------------------------------------

        flat_data["name"] = name

        flat_data["lat"] = lat

        flat_data["lon"] = lon


        # ----------------------------------------------------
        # Timestamp Việt Nam
        # ----------------------------------------------------

        flat_data["timestamp"] = (
            batch_time_iso
        )


        # ----------------------------------------------------
        # Unix timestamp
        # ----------------------------------------------------

        flat_data["timestamp_unix"] = (
            batch_time_unix
        )


        # ----------------------------------------------------
        # Raw JSON
        # ----------------------------------------------------

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


    # ========================================================
    # KHÔNG CÓ DATA
    # ========================================================

    if not records:

        print(
            "No data collected this round."
        )

        return False


    # ========================================================
    # TẠO DATAFRAME
    # ========================================================

    df_new = pd.DataFrame(
        records
    )


    # ========================================================
    # SẮP XẾP CỘT
    # ========================================================

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


    # ========================================================
    # GHI FILE
    # ========================================================

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
            f"Appended "
            f"{len(df_new)} records "
            f"to {OUTPUT_FILE}"
        )


    return True


# ============================================================
# 10. MAIN LOOP - RUN FOREVER
# ============================================================

print()

print(
    "============================================================"
)

print(
    "START CONTINUOUS TRAFFIC COLLECTION"
)

print(
    f"Interval: "
    f"{INTERVAL} seconds "
    f"({INTERVAL // 60} minutes)"
)

print(
    f"Maximum rounds per full day: "
    f"{MAX_ROUNDS}"
)

print(
    "Timezone: Vietnam (UTC+7)"
)

print(
    "Mode: RUN FOREVER"
)

print(
    "============================================================"
)


# ============================================================
# NGÀY DATASET HIỆN TẠI
# ============================================================

current_dataset_date = (
    datetime.now(
        VIETNAM_TIMEZONE
    ).date()
)


# ============================================================
# ROUND COUNTER
#
# round_idx = 0
#     -> chưa thu thập round nào
#
# round_idx = 1
#     -> đã thu thập round 1
#
# ...
#
# Sang ngày mới:
#
#     round_idx = 0
# ============================================================

round_idx = 0


print(
    f"Current dataset date "
    f"(Vietnam): "
    f"{current_dataset_date}"
)

print(
    "Collector will run forever."
)

print(
    "Press Ctrl+C to stop."
)


# ============================================================
# RUN FOREVER
# ============================================================

try:

    while True:

        # ====================================================
        # 1. LẤY THỜI GIAN HIỆN TẠI
        # ====================================================

        now_datetime = datetime.now(
            VIETNAM_TIMEZONE
        )

        now_date = (
            now_datetime.date()
        )


        # ====================================================
        # 2. KIỂM TRA NGÀY MỚI
        # ====================================================

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


            # =================================================
            # 3. UPLOAD DATASET NGÀY CŨ
            # =================================================

            upload_success = (
                upload_daily_dataset(
                    OUTPUT_FILE,
                    current_dataset_date
                )
            )


            # =================================================
            # 4. UPLOAD THÀNH CÔNG
            # =================================================

            if upload_success:

                # ---------------------------------------------
                # Xóa file local
                # ---------------------------------------------

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
                        "WARNING: Could not remove "
                        f"local file: {e}"
                    )


                # ---------------------------------------------
                # Chuyển sang ngày mới
                # ---------------------------------------------

                current_dataset_date = (
                    now_date
                )


                # ---------------------------------------------
                # RESET ROUND
                # ---------------------------------------------

                round_idx = 0


                print(
                    f"Started new dataset date: "
                    f"{current_dataset_date}"
                )

                print(
                    "Round counter reset to 0."
                )


            # =================================================
            # 5. UPLOAD THẤT BẠI
            # =================================================

            else:

                print(
                    "WARNING: Previous dataset "
                    "was NOT uploaded successfully."
                )

                print(
                    "Keeping local dataset."
                )

                print(
                    "Will retry upload in 60 seconds."
                )


                # ---------------------------------------------
                # Không chuyển sang ngày mới.
                #
                # Không collect data mới.
                #
                # Tránh trộn dataset của hai ngày.
                # ---------------------------------------------

                time.sleep(60)

                continue


        # ====================================================
        # 6. TĂNG ROUND
        # ====================================================

        round_idx += 1


        # ====================================================
        # 7. HIỂN THỊ THÔNG TIN
        # ====================================================

        print()

        print(
            "============================================================"
        )

        print(
            f"DATASET DATE : "
            f"{current_dataset_date}"
        )

        print(
            f"ROUND        : "
            f"{round_idx}/{MAX_ROUNDS}"
        )

        print(
            f"TIME         : "
            f"{now_datetime.isoformat()}"
        )

        print(
            "============================================================"
        )


        # ====================================================
        # 8. COLLECT DATA
        # ====================================================

        start_time = time.time()

        try:

            collect_success = (
                collect_once()
            )

        except Exception as e:

            print(
                "Error in collect_once():",
                e
            )

            collect_success = False


        # ====================================================
        # 9. THỜI GIAN ROUND
        # ====================================================

        elapsed = (
            time.time() - start_time
        )


        # ====================================================
        # 10. SLEEP ĐẾN ROUND TIẾP THEO
        # ====================================================

        sleep_time = max(
            0,
            INTERVAL - elapsed
        )


        print()

        print(
            f"Round {round_idx} completed."
        )

        print(
            f"Collect success: "
            f"{collect_success}"
        )

        print(
            f"Round elapsed: "
            f"{elapsed:.2f}s"
        )

        print(
            f"Sleeping: "
            f"{sleep_time:.2f}s"
        )


        time.sleep(
            sleep_time
        )


# ============================================================
# STOP BẰNG CTRL+C
# ============================================================

except KeyboardInterrupt:

    print()

    print(
        "============================================================"
    )

    print(
        "Collector stopped by user."
    )

    print(
        f"Current dataset date: "
        f"{current_dataset_date}"
    )

    print(
        f"Current round: "
        f"{round_idx}"
    )

    print(
        "============================================================"
    )