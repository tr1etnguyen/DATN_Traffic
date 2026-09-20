import pandas as pd
import hashlib

# =============================
# 0. Load data (Thêm low_memory=False để bỏ qua DtypeWarning)
# =============================
print("Đang đọc dữ liệu...")
df = pd.read_csv("cleaned.csv", low_memory=False)

# =============================
# 0.5. Xóa các cột không cần thiết
# =============================
cols_to_drop = ["node_id", "lat", "lon"]
df.drop(columns=cols_to_drop, errors='ignore', inplace=True)
print(f"Đã xóa các cột: {cols_to_drop} (nếu có).")

# =============================
# 1. Lấy tất cả cột tọa độ
# =============================
coord_cols = [col for col in df.columns if "coordinate" in col]
print(f"Number of coordinate columns: {len(coord_cols)}")

# =============================
# 2. Tạo hash cho geometry
# =============================
def hash_geometry(row):
    coords = row[coord_cols].values
    # Thay vì dùng map str cho toàn bộ, ta xử lý an toàn hơn để tránh NaN ảnh hưởng
    coords_str = ",".join([str(c) for c in coords if pd.notna(c)])
    return hashlib.md5(coords_str.encode()).hexdigest()

print("Đang băm (hash) tọa độ...")
df["geometry_hash"] = df.apply(hash_geometry, axis=1)

# =============================
# 3. Xử lý time nếu có
# =============================
if "timestamp" in df.columns:
    print("Đang xử lý thời gian...")
    
    # BỔ SUNG QUAN TRỌNG: Đồng nhất chữ 'T' thành khoảng trắng trước khi ép kiểu
    df["timestamp"] = df["timestamp"].astype(str).str.replace("T", " ")
    
    # Thêm errors='coerce' để ép các giá trị lỗi (như 106.638...) thành NaT (Not a Time)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce')
    
    # Kiểm tra xem có bao nhiêu dòng bị lỗi lệch cột (đã sửa lỗi thiếu dấu #)
    invalid_time_count = df["timestamp"].isna().sum()
    if invalid_time_count > 0:
        print(f"Cảnh báo: Phát hiện {invalid_time_count} dòng bị lỗi cấu trúc (lệch cột hoặc sai định dạng). Sẽ tự động loại bỏ.")
        # Xóa các dòng bị lỗi cấu trúc
        df = df.dropna(subset=["timestamp"])
        
    df["time_window"] = df["timestamp"].dt.floor("5min")
    key_cols = ["geometry_hash", "time_window"]
else:
    key_cols = ["geometry_hash"]

# =============================
# 4. Remove duplicate
# =============================
print("Đang xóa dữ liệu trùng lặp...")
df_clean = df.drop_duplicates(subset=key_cols)

print("--- THỐNG KÊ ---")
print(f"Before: {len(df) + invalid_time_count if 'invalid_time_count' in locals() else len(df)}")
print(f"After cleaning & deduplicating: {len(df_clean)}")

# Save
df_clean.to_csv("cleaned.csv", index=False)
print("Đã lưu kết quả ra file cleaned.csv")