import pandas as pd
import pyodbc
import os
from math import ceil

# =========================
# 📂 檔案路徑
# =========================
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "dataset")

articles_file = os.path.join(DATA_DIR, "articles.csv")
customers_file = os.path.join(DATA_DIR, "customers.csv")
transactions_file = os.path.join(DATA_DIR, "transactions_train.csv")

# =========================
# 🔗 SQL 連線
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
cursor = conn.cursor()
print("✅ SQL 連線成功")


# =========================
# 📂 CSV 安全讀取
# =========================
df_articles = pd.read_csv(articles_file, dtype=str)
df_customers = pd.read_csv(customers_file, dtype=str)
df_transactions = pd.read_csv(transactions_file, dtype=str)  # 🔥 直接完整讀取以方便清洗

print("✅ CSV 讀取完成")

# =========================
# 🧼 資料清洗（🔥加強版） 
# =========================
def clean_df(df):
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    # 🔥 統一空值
    df = df.replace({
        'nan': None,
        'None': None,
        '': None,
        'NA': None,
        'null': None
    })
    return df

df_articles = clean_df(df_articles)
df_customers = clean_df(df_customers)
df_transactions = clean_df(df_transactions)

# 🔥 型別轉換
df_customers['age'] = pd.to_numeric(df_customers['age'], errors='coerce')
df_transactions['price'] = pd.to_numeric(df_transactions['price'], errors='coerce')
df_transactions['sales_channel_id'] = pd.to_numeric(df_transactions['sales_channel_id'], errors='coerce')
df_transactions['t_dat'] = pd.to_datetime(df_transactions['t_dat'], errors='coerce')

print("✅ 資料清洗完成")

# =========================
# 🏷 表格 & PK
# =========================
tables = {
    "articles": df_articles,
    "customers": df_customers,
    "transactions": df_transactions
}

primary_keys = {
    "articles": "article_id",
    "customers": "customer_id",
    "transactions": "transaction_id"  # 🔥 這裡會用 IDENTITY
}

# =========================
# 💾 輸出 CSV
# =========================
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

for name, df in tables.items():
    df.to_csv(os.path.join(OUTPUT_DIR, f"{name}.csv"), index=False)

print("✅ CSV 輸出完成")

# =========================
# 🔢 型別對應
# =========================
type_map = {
    "article_id": "VARCHAR(20)",
    "customer_id": "VARCHAR(64)",
    "age": "INT",
    "price": "DECIMAL(10,4)",
    "sales_channel_id": "INT",
    "t_dat": "DATE",
    "detail_desc": "VARCHAR(MAX)"
}

# =========================
# 🛑 刪表
# =========================
for t in ["transactions", "customers", "articles"]:
    cursor.execute(f"IF OBJECT_ID('{t}', 'U') IS NOT NULL DROP TABLE [{t}]")
conn.commit()
print("✅ 舊表刪除完成")

# =========================
# 🏗 建表函數（改良版）
# =========================
def create_table(name, df, pk=None):
    cols = []
    
    # 🔥 針對 transactions 的 transaction_id 自動生成
    if name == "transactions":
        cols.append("[transaction_id] INT IDENTITY(1,1) PRIMARY KEY")
    
    for c in df.columns:
        if name == "transactions" and c == "transaction_id":
            continue  # 已由 IDENTITY 生成
        # 判斷型別
        if c == "detail_desc":
            sql_type = "VARCHAR(MAX)"
        elif df[c].astype(str).str.len().max() > 255:
            sql_type = "VARCHAR(MAX)"
        else:
            sql_type = type_map.get(c, "VARCHAR(255)")
        cols.append(f"[{c}] {sql_type}")
    
    # 🔥 一般表格主鍵檢查
    if pk and name != "transactions" and pk in df.columns:
        cols.append(f"PRIMARY KEY ([{pk}])")
    
    sql = f"CREATE TABLE [{name}] ({','.join(cols)})"
    cursor.execute(sql)
    conn.commit()
    print(f"✅ 建表 {name}")

# =========================
# ⚡ 批次寫入函數（保留原邏輯）
# =========================
def insert_df(df, table_name, batch_size=1000):
    df = clean_df(df)
    df = df.where(pd.notnull(df), None)

    for col in df.columns:
        if col in ["age", "price", "sales_channel_id"]:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 🔥 transactions 不插入 transaction_id
    insert_cols = [c for c in df.columns if not (table_name=="transactions" and c=="transaction_id")]

    cols_sql = ",".join(f"[{c}]" for c in insert_cols)
    placeholders = ",".join("?" for _ in insert_cols)
    sql = f"INSERT INTO [{table_name}] ({cols_sql}) VALUES ({placeholders})"

    total = len(df)
    batches = (total + batch_size - 1) // batch_size
    cursor.fast_executemany = True

    print(f"📊 {table_name} 原始筆數: {total}")

    for i in range(batches):
        batch = df.iloc[i*batch_size:(i+1)*batch_size]

        data = []
        for row in batch.itertuples(index=False, name=None):
            row_dict = dict(zip(df.columns, row))
            new_row = []

            for col in insert_cols:
                val = row_dict[col]

                if val is None or val == '' or val == 'nan':
                    new_row.append(None)
                elif isinstance(val, float):
                    if pd.isna(val) or val in [float('inf'), float('-inf')]:
                        new_row.append(None)
                    else:
                        new_row.append(float(val))
                else:
                    new_row.append(val)

            data.append(tuple(new_row))

        try:
            cursor.executemany(sql, data)
            conn.commit()

        except Exception as e:
            print(f"\n❌ {table_name} 批次錯誤（第 {i} 批）:", e)
            print("👉 開始精準定位錯誤資料...")

            # 🔥 找出哪一筆錯（不寫入）
            for j, row in enumerate(data):
                try:
                    cursor.execute(sql, row)
                except Exception as err:
                    print("\n🚨 發現錯誤資料！")
                    print(f"📍 表: {table_name}")
                    print(f"📍 批次: {i}")
                    print(f"📍 全域筆數: {i*batch_size + j}")
                    print("❌ 錯誤原因:", err)
                    print("📦 問題資料:", row)

                    # 🔥 直接終止（不允許少資料）
                    raise Exception("ETL 中止：請修正資料後重新執行")

            # 理論不會走到
            raise Exception("未知錯誤（批次錯但找不到單筆）")

        percent = ((i+1)/batches)*100
        print(f"⏳ {table_name}: {percent:.2f}%", end="\r")

    print(f"\n✅ {table_name} 完成（100% 無遺失）")

# =========================
# 🔗 FK 建立
# =========================
def create_fk(child, col, parent, parent_col):
    try:
        cursor.execute(f"""
        ALTER TABLE [{child}]
        ADD CONSTRAINT FK_{child}_{col}
        FOREIGN KEY ([{col}]) REFERENCES [{parent}]([{parent_col}])
        """)
        conn.commit()
        print(f"🔗 FK {child}->{parent}")
    except Exception as e:
        print(f"⚠️ FK 失敗: {e}")

# =========================
# 🚀 建表 + 寫入
# =========================
for name, df in tables.items():
    create_table(name, df, primary_keys.get(name))

for name, df in tables.items():
    insert_df(df, name)

# =========================
# 🔗 FK
# =========================
create_fk("transactions","customer_id","customers","customer_id")
create_fk("transactions","article_id","articles","article_id")

print("🎉 ETL 完成")

# =========================
# 📊 報表
# =========================
for name, df in tables.items():
    missing = df.isnull().sum()
    report = pd.DataFrame({
        "欄位": missing.index,
        "缺值數": missing.values,
        "缺值比例": missing.values / len(df)
    })
    report.to_csv(os.path.join(OUTPUT_DIR, f"{name}_missing.csv"), index=False)

print("📊 報表完成")