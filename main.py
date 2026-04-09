import pandas as pd
import pyodbc
import os

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
database = 'project'
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
# 🧼 清洗函數
# =========================
def clean_df(df):
    df.columns = df.columns.str.strip().str.lower()
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    df = df.replace({'nan': None, '': None})
    return df

# =========================
# 🏗 建表
# =========================
def create_tables():
    cursor.execute("IF OBJECT_ID('transactions','U') IS NOT NULL DROP TABLE transactions")
    cursor.execute("IF OBJECT_ID('customers','U') IS NOT NULL DROP TABLE customers")
    cursor.execute("IF OBJECT_ID('articles','U') IS NOT NULL DROP TABLE articles")

    cursor.execute("""
    CREATE TABLE articles (
        article_id VARCHAR(20) PRIMARY KEY,
        product_code VARCHAR(20),
        prod_name VARCHAR(255),
        product_type_name VARCHAR(100),
        product_group_name VARCHAR(100),
        colour_group_name VARCHAR(50),
        department_name VARCHAR(100),
        index_name VARCHAR(50),
        section_name VARCHAR(100),
        garment_group_name VARCHAR(100)
    )
    """)

    cursor.execute("""
    CREATE TABLE customers (
        customer_id VARCHAR(64) PRIMARY KEY,
        club_member_status VARCHAR(50),
        fashion_news_frequency VARCHAR(50),
        age INT,
        postal_code VARCHAR(64)
    )
    """)

    cursor.execute("""
    CREATE TABLE transactions (
        transaction_id INT IDENTITY(1,1) PRIMARY KEY,
        t_dat DATE,
        customer_id VARCHAR(64),
        article_id VARCHAR(20),
        price DECIMAL(10,4),
        sales_channel_id INT
    )
    """)

    conn.commit()

create_tables()

# =========================
# ⚡ INSERT FUNCTION（通用）
# =========================
def insert_batch(df, table):
    cols = ",".join(df.columns)
    placeholders = ",".join("?" * len(df.columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    data = [tuple(x) for x in df.itertuples(index=False, name=None)]
    
    try:
        cursor.executemany(sql, data)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"❌ Error in {table}: {e}")

# =========================
# 🚀 1️⃣ articles（小表可一次）
# =========================
df_articles = pd.read_csv(articles_file, dtype=str)
df_articles = clean_df(df_articles)

df_articles = df_articles[[
    'article_id','product_code','prod_name',
    'product_type_name','product_group_name',
    'colour_group_name','department_name',
    'index_name','section_name','garment_group_name'
]].drop_duplicates()

insert_batch(df_articles, "articles")
print("✅ articles 完成")

# =========================
# 🚀 2️⃣ customers（小表）
# =========================
df_customers = pd.read_csv(customers_file, dtype=str)
df_customers = clean_df(df_customers)

df_customers = df_customers[[
    'customer_id','club_member_status',
    'fashion_news_frequency','age','postal_code'
]].drop_duplicates()

df_customers['age'] = pd.to_numeric(df_customers['age'], errors='coerce')

insert_batch(df_customers, "customers")
print("✅ customers 完成")

# =========================
# 🚀 3️⃣ transactions（大表🔥 chunk）
# =========================
chunksize = 100000

for chunk in pd.read_csv(transactions_file, chunksize=chunksize):
    chunk = clean_df(chunk)

    chunk = chunk[[
        't_dat','customer_id','article_id','price','sales_channel_id'
    ]]

    # 型別轉換
    chunk['t_dat'] = pd.to_datetime(chunk['t_dat'], errors='coerce')
    chunk['price'] = pd.to_numeric(chunk['price'], errors='coerce')
    chunk['sales_channel_id'] = pd.to_numeric(chunk['sales_channel_id'], errors='coerce')

    insert_batch(chunk, "transactions")
    print("⏳ transactions chunk 完成")

# =========================
# 🔗 FK（最後建立）
# =========================
cursor.execute("""
ALTER TABLE transactions
ADD CONSTRAINT FK_customer FOREIGN KEY (customer_id)
REFERENCES customers(customer_id)
""")

cursor.execute("""
ALTER TABLE transactions
ADD CONSTRAINT FK_article FOREIGN KEY (article_id)
REFERENCES articles(article_id)
""")

conn.commit()

print("🎉 ETL 完成（升級版）")