import os
import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = r"C:\HM"
table = "clean_transactions"

output_file = os.path.join(DATA_DIR, f"{table}_ALL.parquet")

# =========================
# 📂 取得檔案（排除 ALL 自己）
# =========================
files = sorted([
    os.path.join(DATA_DIR, f)
    for f in os.listdir(DATA_DIR)
    if f.startswith(table)
    and f.endswith(".parquet")
    and f != f"{table}_ALL.parquet"   # 🔥 防止自己被讀進去
])

print(f"📂 找到 {len(files)} 個檔案")

if len(files) == 0:
    raise ValueError("❌ 沒有找到可合併的 parquet 檔案")

# =========================
# 🔍 過濾壞檔（防止 magic bytes error）
# =========================
valid_files = []

for f in files:
    try:
        pq.ParquetFile(f)  # 測試能否讀取
        valid_files.append(f)
    except Exception as e:
        print(f"⚠️ 跳過壞檔: {f} -> {e}")

files = valid_files

print(f"📂 最終可用檔案數: {len(files)}")

# =========================
# 📌 schema 基準
# =========================
first_pf = pq.ParquetFile(files[0])
base_schema = first_pf.schema_arrow

print("📌 使用統一 schema:", base_schema)

writer = None
total_rows = 0

# =========================
# 🔥 合併寫入
# =========================
for file in files:
    print(f"🔄 處理: {file}")

    pf = pq.ParquetFile(file)

    for batch in pf.iter_batches():
        table_batch = pa.Table.from_batches([batch])

        # 強制統一 schema
        table_batch = table_batch.cast(base_schema)

        total_rows += table_batch.num_rows

        if writer is None:
            writer = pq.ParquetWriter(output_file, base_schema)

        writer.write_table(table_batch)

# =========================
# 🔚 關閉 writer
# =========================
if writer:
    writer.close()

print("🎉 合併完成！")
print(f"📁 輸出: {output_file}")
print(f"📊 總筆數: {total_rows:,}")