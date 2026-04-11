import pandas as pd
import pyodbc
import os
import sys

# =========================
# 🔌 SQL 連線
# =========================
server = 'linpeichunhappy.database.windows.net'
database = 'HM2'
username = 'missa'
password = 'Cc12345678'
driver = '{ODBC Driver 18 for SQL Server}'

conn = pyodbc.connect(
    f'DRIVER={driver};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
    'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
)

print("✅ SQL 連線成功")

# =========================
# 📂 輸出路徑
# =========================
DATA_DIR = r"C:\HM"
os.makedirs(DATA_DIR, exist_ok=True)

tables = ["clean_transactions"]
chunksize = 100_000

# =========================
# 🚀 ETL 主流程
# =========================
for table in tables:
    print(f"\n🚀 匯出: {table}")

    # 📊 總筆數
    total_rows = pd.read_sql(f"SELECT COUNT(*) FROM {table}", conn).iloc[0, 0]
    print(f"📊 總筆數: {total_rows:,}")

    processed = 0

    # 🔥 chunk 讀取
    df_iter = pd.read_sql(
        f"SELECT * FROM {table}",
        conn,
        chunksize=chunksize
    )

    for i, chunk in enumerate(df_iter):
        output_path = os.path.join(DATA_DIR, f"{table}_{i}.parquet")

        # 💾 寫 parquet
        chunk.to_parquet(
            output_path,
            engine="pyarrow",
            compression="snappy",
            index=False
        )

        # 📦 更新進度
        processed += len(chunk)
        percent = processed / total_rows * 100

        sys.stdout.write(
            f"\r📦 已處理: {processed:,} / {total_rows:,} ({percent:.2f}%)"
        )
        sys.stdout.flush()

# =========================
# 🎉 結束
# =========================
print("\n🎉 全部轉換完成！")
conn.close()