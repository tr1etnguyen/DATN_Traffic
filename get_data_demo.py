import requests
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
import os
import json

from google.cloud import storage


# ============================================================
# 1. TOMTOM API KEYS
# ============================================================

# KHÔNG hard-code API keys trong source code.
#
# Trên Linux VM:
#
# export TOMTOM_API_KEYS="cnxiHKTvclKQwSMpWtPoJjLZgJ3YNb2C,BhvsX98HvjLMlDRFXLidX7nx0V2eChUc,ncWSZE8vd6HyWudDai4UPx3rl5KDrU8F,sm7UsMitEY9h5ARcP9pognJvm2C2kZHY,o8qMdUxmXRQPZ70RZs9r1QWjt2D0hDOP,Nqnl6ug5Rz8Eb5X5sdPTRdbVLWlyFrE5"
#
# Nếu có nhiều key, ngăn cách bằng dấu phẩy.

if "TOMTOM_API_KEYS" not in os.environ:
    raise RuntimeError(
        "Environment variable TOMTOM_API_KEYS "
        "chưa được thiết lập."
    )

API_KEYS = [
    key.strip()
    for key in os.environ["TOMTOM_API_KEYS"].split(",")
    if key.strip()
]

if not API_KEYS:
    raise RuntimeError(
        "Không tìm thấy TomTom API key."
    )

current_key_index = 0


# ============================================================
# 2. FILE CONFIGURATION
# ============================================================

# File JSON chứa nodes
#
# {
#     "nodes": [
#         {
#             "name": "...",
#             "lat": ...,
#             "lon": ...
#         }
#     ],
#     "connections": [...]
# }

INPUT_FILE = "target.json"

# Dataset hiện tại trên VM
OUTPUT_FILE = "cleaned.csv"


# ============================================================
# 3. DEMO CONFIGURATION
# ============================================================

# ------------------------------------------------------------
# DEMO:
#
# Mỗi 30 giây lấy một batch.
#
# Production thực tế:
#
# INTERVAL = 300
#
# ------------------------------------------------------------

INTERVAL = 30


# ------------------------------------------------------------
# Demo chạy trong 5 phút
# ------------------------------------------------------------

DEMO_DURATION = 5 * 60


# ------------------------------------------------------------
# Sau 2 phút giả lập việc qua 00:00
# ------------------------------------------------------------

DEMO_SWITCH_AFTER = 2 * 60


# ============================================================
# 4. TIMEZONE VIỆT NAM
# ============================================================

VIETNAM_TIMEZONE = timezone(
    timedelta(hours=7)
)


# ============================================================
# 5. GOOGLE CLOUD STORAGE CONFIGURATION
# ============================================================

# ------------------------------------------------------------
# THAY BẰNG TÊN BUCKET CỦA BẠN
# ------------------------------------------------------------

GCS_BUCKET_NAME = "tomtom_dataset"


# ------------------------------------------------------------
# Folder trên GCS
# ------------------------------------------------------------
#
# Kết quả:
#
# gs://tomtom_dataset/
# └── traffic/
#     └── 2026-10-08/
#         └── traffic_2026-10-08.csv
#
# ------------------------------------------------------------

GCS_PREFIX = "traffic"


# ============================================================
# 6. API KEY MANAGEMENT
# ============================================================

def get_current_key():

    return API_KEYS[current_key_index]


def switch_key():

    global current_key_index

    current_key_index += 1

    if current_key_index >= len(API_KEYS):

        print(
            "All API keys exhausted."
        )

        print(
            "Waiting 5 minutes before resetting..."
        )

        time.sleep(300)

        current_key_index = 0

    print(
        f"Switched to API KEY "
        f"#{current_key_index + 1}"
    )


# ============================================================
# 7. CALL TOMTOM TRAFFIC FLOW API
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
            # API KEY HẾT QUOTA / BỊ TỪ CHỐI
            # ------------------------------------------------

            if res.status_code in [403, 429]:

                print(
                    f"Key #{current_key_index + 1} exhausted "
                    f"(HTTP {res.status_code})"
                )

                switch_key()

                continue

            # ------------------------------------------------
            # HTTP ERROR KHÁC
            # ------------------------------------------------

            if res.status_code != 200:

                print(
                    "HTTP Error:",
                    res.status_code
                )

                return None

            # ------------------------------------------------
            # PARSE JSON
            # ------------------------------------------------

            data = res.json()

            # ------------------------------------------------
            # API ERROR BÊN TRONG JSON
            # ------------------------------------------------

            if "error" in data:

                print(
                    "API Error:",
                    data["error"]
                )

                switch_key()

                continue

            # ------------------------------------------------
            # KIỂM TRA FLOW DATA
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
# 8. FLATTEN JSON
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
# 9. LOAD NODES FROM JSON
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
    f"Loaded nodes from JSON: {len(nodes)}"
)


# ============================================================
# 10. VALIDATE NODE STRUCTURE
# ============================================================

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


# ============================================================
# 11. CREATE NODE DATAFRAME
# ============================================================

df_nodes = pd.DataFrame(
    nodes
)


print()
print("Nodes:")

print(
    df_nodes[
        ["name", "lat", "lon"]
    ].to_string(index=False)
)


# ============================================================
# 12. UPLOAD DAILY DATASET TO GCS
# ============================================================

def upload_daily_dataset(
    local_file,
    dataset_date
):

    print()
    print(
        "=" * 70
    )

    print(
        f"Uploading dataset for date: "
        f"{dataset_date}"
    )

    print(
        f"Local file: "
        f"{local_file}"
    )

    print(
        "=" * 70
    )

    # --------------------------------------------------------
    # Kiểm tra file
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
            "File is empty."
        )

        return False

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
        # Tạo đường dẫn object
        # ----------------------------------------------------

        destination_blob = (
            f"{GCS_PREFIX}/"
            f"{dataset_date}/"
            f"traffic_{dataset_date}.csv"
        )

        print(
            f"Destination:"
        )

        print(
            f"gs://{GCS_BUCKET_NAME}/"
            f"{destination_blob}"
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

        print()
        print(
            "UPLOAD SUCCESSFUL!"
        )

        print(
            f"gs://{GCS_BUCKET_NAME}/"
            f"{destination_blob}"
        )

        return True

    except Exception as e:

        print()
        print(
            "UPLOAD FAILED!"
        )

        print(
            "Error:",
            e
        )

        return False


# ============================================================
# 13. COLLECT DATA FOR ONE ROUND
# ============================================================

def collect_once():

    records = []

    # --------------------------------------------------------
    # Thời gian Việt Nam
    # --------------------------------------------------------

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

    print()
    print(
        f"Batch time: "
        f"{batch_time_iso}"
    )

    # ========================================================
    # LOOP THROUGH NODES
    # ========================================================

    for _, row in df_nodes.iterrows():

        # ----------------------------------------------------
        # Node information
        # ----------------------------------------------------

        name = row["name"]

        lat = float(
            row["lat"]
        )

        lon = float(
            row["lon"]
        )

        # ----------------------------------------------------
        # Retry cho đến khi lấy được dữ liệu
        # ----------------------------------------------------

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
        # Flatten TomTom JSON
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

        # ----------------------------------------------------
        # Delay giữa các request
        # ----------------------------------------------------

        time.sleep(0.1)

    # ========================================================
    # CHECK RESULT
    # ========================================================

    if not records:

        print(
            "No data collected this round."
        )

        return

    # ========================================================
    # CREATE DATAFRAME
    # ========================================================

    df_new = pd.DataFrame(
        records
    )

    # ========================================================
    # METADATA COLUMNS
    # ========================================================

    metadata_columns = [
        "name",
        "lat",
        "lon"
    ]

    # --------------------------------------------------------
    # Các cột còn lại
    # --------------------------------------------------------

    other_columns = [
        column
        for column in df_new.columns
        if column not in metadata_columns
    ]

    # --------------------------------------------------------
    # Đưa metadata lên đầu
    # --------------------------------------------------------

    df_new = df_new[
        metadata_columns + other_columns
    ]

    # ========================================================
    # APPEND TO CSV
    # ========================================================

    if not os.path.exists(
        OUTPUT_FILE
    ):

        df_new.to_csv(
            OUTPUT_FILE,
            index=False,
            encoding="utf-8-sig"
        )

        print()
        print(
            f"Created new file: "
            f"{OUTPUT_FILE}"
        )

        print(
            f"Records: "
            f"{len(df_new)}"
        )

    else:

        df_new.to_csv(
            OUTPUT_FILE,
            mode="a",
            header=False,
            index=False,
            encoding="utf-8-sig"
        )

        print()
        print(
            f"Appended "
            f"{len(df_new)} records "
            f"to {OUTPUT_FILE}"
        )


# ============================================================
# 14. START 5-MINUTE DEMO
# ============================================================

print()
print(
    "=" * 70
)

print(
    "STARTING 5-MINUTE GCS DEMO"
)

print(
    "=" * 70
)

print(
    f"Vietnam timezone: UTC+7"
)

print(
    f"Collection interval: "
    f"{INTERVAL} seconds"
)

print(
    f"Demo duration: "
    f"{DEMO_DURATION} seconds"
)

print(
    f"Simulated day change after: "
    f"{DEMO_SWITCH_AFTER} seconds"
)

print(
    f"GCS bucket: "
    f"{GCS_BUCKET_NAME}"
)

print(
    "=" * 70
)


# ============================================================
# 15. INITIAL DATASET DATE
# ============================================================

current_dataset_date = (
    datetime.now(
        VIETNAM_TIMEZONE
    ).date()
)


print(
    f"Initial dataset date "
    f"(Vietnam): "
    f"{current_dataset_date}"
)


# ============================================================
# 16. DEMO VARIABLES
# ============================================================

demo_start_time = time.time()

round_idx = 0

# ------------------------------------------------------------
# Quan trọng:
#
# Đảm bảo việc giả lập qua ngày mới chỉ xảy ra MỘT LẦN.
# ------------------------------------------------------------

demo_day_switched = False


# ============================================================
# 17. MAIN DEMO LOOP
# ============================================================

while True:

    # --------------------------------------------------------
    # Thời gian demo đã chạy
    # --------------------------------------------------------

    demo_elapsed = (
        time.time()
        - demo_start_time
    )

    # --------------------------------------------------------
    # Kiểm tra hết 5 phút
    # --------------------------------------------------------

    if demo_elapsed >= DEMO_DURATION:

        break

    round_idx += 1

    print()
    print(
        "=" * 70
    )

    print(
        f"ROUND {round_idx}"
    )

    print(
        f"Demo elapsed: "
        f"{demo_elapsed:.2f}s / "
        f"{DEMO_DURATION}s"
    )

    print(
        "=" * 70
    )

    start_time = time.time()

    try:

        # ====================================================
        # NGÀY THỰC TẾ THEO VIỆT NAM
        # ====================================================

        real_now_date = (
            datetime.now(
                VIETNAM_TIMEZONE
            ).date()
        )


        # ====================================================
        # XÁC ĐỊNH NGÀY DATASET
        # ====================================================

        if (
            demo_elapsed >= DEMO_SWITCH_AFTER
            and not demo_day_switched
        ):

            # ------------------------------------------------
            # DEMO:
            #
            # Giả lập qua 00:00
            # ------------------------------------------------

            now_date = (
                current_dataset_date
                + timedelta(days=1)
            )

            demo_day_switched = True

            print()
            print(
                "[DEMO] Simulating "
                "00:00 Vietnam time..."
            )

            print(
                f"[DEMO] Real Vietnam date: "
                f"{real_now_date}"
            )

            print(
                f"[DEMO] Simulated new date: "
                f"{now_date}"
            )

        else:

            now_date = (
                real_now_date
            )


        # ====================================================
        # CHECK NEW DATASET DAY
        # ====================================================

        if (
            now_date
            != current_dataset_date
        ):

            print()
            print(
                "=" * 70
            )

            print(
                "NEW DATASET DAY DETECTED!"
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
                "=" * 70
            )


            # =================================================
            # UPLOAD PREVIOUS DATASET
            # =================================================

            if os.path.exists(
                OUTPUT_FILE
            ):

                print()
                print(
                    "Uploading previous dataset..."
                )

                upload_success = (
                    upload_daily_dataset(
                        OUTPUT_FILE,
                        current_dataset_date
                    )
                )

            else:

                print(
                    "No local dataset found."
                )

                upload_success = True


            # =================================================
            # HANDLE UPLOAD RESULT
            # =================================================

            if upload_success:

                # ------------------------------------------------
                # Upload thành công
                # ------------------------------------------------

                if os.path.exists(
                    OUTPUT_FILE
                ):

                    os.remove(
                        OUTPUT_FILE
                    )

                    print(
                        f"Removed local dataset: "
                        f"{OUTPUT_FILE}"
                    )

                print()
                print(
                    "Previous dataset archived "
                    "successfully."
                )


                # ------------------------------------------------
                # Chuyển sang ngày mới
                # ------------------------------------------------

                current_dataset_date = (
                    now_date
                )

                print(
                    f"Started new dataset date: "
                    f"{current_dataset_date}"
                )

            else:

                # ------------------------------------------------
                # Upload thất bại
                #
                # KHÔNG xóa file
                # KHÔNG chuyển dataset
                # ------------------------------------------------

                print()
                print(
                    "WARNING:"
                )

                print(
                    "Previous dataset was NOT "
                    "uploaded successfully."
                )

                print(
                    "Local dataset will be kept."
                )

                print(
                    f"Dataset date remains: "
                    f"{current_dataset_date}"
                )

                # ------------------------------------------------
                # Để tránh trộn ngày mới vào dataset cũ,
                # demo sẽ tiếp tục dùng ngày cũ.
                # ------------------------------------------------

                now_date = (
                    current_dataset_date
                )


        # ====================================================
        # COLLECT CURRENT ROUND
        # ====================================================

        collect_once()


    except Exception as e:

        print()
        print(
            "ERROR IN DEMO:"
        )

        print(e)


    # ========================================================
    # ROUND ELAPSED
    # ========================================================

    elapsed = (
        time.time()
        - start_time
    )


    # ========================================================
    # DEMO ELAPSED
    # ========================================================

    demo_elapsed = (
        time.time()
        - demo_start_time
    )


    # ========================================================
    # CHECK DEMO FINISHED
    # ========================================================

    if (
        demo_elapsed
        >= DEMO_DURATION
    ):

        break


    # ========================================================
    # WAIT FOR NEXT ROUND
    # ========================================================

    sleep_time = min(
        INTERVAL - elapsed,
        DEMO_DURATION - demo_elapsed
    )

    sleep_time = max(
        0,
        sleep_time
    )

    print()
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
# 18. DEMO FINISHED
# ============================================================

total_demo_time = (
    time.time()
    - demo_start_time
)


print()
print(
    "=" * 70
)

print(
    "5-MINUTE DEMO COMPLETED"
)

print(
    "=" * 70
)

print(
    f"Total runtime: "
    f"{total_demo_time:.2f} seconds"
)

print(
    f"Total rounds: "
    f"{round_idx}"
)

print(
    f"Current dataset date: "
    f"{current_dataset_date}"
)

print(
    "=" * 70
)


# ============================================================
# 19. FINAL LOCAL FILE STATUS
# ============================================================

if os.path.exists(
    OUTPUT_FILE
):

    print(
        f"Current local dataset: "
        f"{OUTPUT_FILE}"
    )

    print(
        f"File size: "
        f"{os.path.getsize(OUTPUT_FILE)} bytes"
    )

else:

    print(
        f"No local {OUTPUT_FILE} found."
    )

print()
print(
    "Demo finished."
)