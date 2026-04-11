import pandas as pd
import pyodbc
import os

# =========================
# 📁 路徑設定
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
cursor.fast_executemany = True
print("✅ SQL 連線成功")

# =========================
# 🧼 清洗函數
# =========================
def clean_df(df):
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    df = df.replace({'nan': None, 'None': None, '': None})
    return df

# =========================
# 🏗 建表函數
# =========================
def create_table(sql):
    cursor.execute(sql)
    conn.commit()

# =========================
# 1️⃣ articles
# =========================
create_table("""
IF OBJECT_ID('articles','U') IS NOT NULL DROP TABLE articles
""")
create_table("""
CREATE TABLE articles (
    article_id VARCHAR(20) PRIMARY KEY,
    product_code VARCHAR(20),
    prod_name VARCHAR(255),
    product_type_no VARCHAR(10),
    product_type_name VARCHAR(100),
    product_group_name VARCHAR(100),
    graphical_appearance_no VARCHAR(10),
    graphical_appearance_name VARCHAR(50),
    colour_group_code VARCHAR(10),
    colour_group_name VARCHAR(50),
    perceived_colour_value_id VARCHAR(10),
    perceived_colour_value_name VARCHAR(50),
    perceived_colour_master_id VARCHAR(10),
    perceived_colour_master_name VARCHAR(50),
    department_no VARCHAR(10),
    department_name VARCHAR(100),
    index_code VARCHAR(10),
    index_name VARCHAR(50),
    index_group_no VARCHAR(10),
    index_group_name VARCHAR(50),
    section_no VARCHAR(10),
    section_name VARCHAR(50),
    garment_group_no VARCHAR(10),
    garment_group_name VARCHAR(50),
    detail_desc VARCHAR(255)
)
""")

df_articles = pd.read_csv(articles_file, dtype=str)
df_articles = clean_df(df_articles)

cols = ",".join(f"[{c}]" for c in df_articles.columns)
placeholders = ",".join("?" for _ in df_articles.columns)
sql_insert = f"INSERT INTO articles ({cols}) VALUES ({placeholders})"
cursor.executemany(sql_insert, [tuple(x) for x in df_articles.fillna('').values])
conn.commit()
print("✅ articles 寫入完成")

# =========================
# 2️⃣ customers
# =========================
create_table("""
IF OBJECT_ID('customers','U') IS NOT NULL DROP TABLE customers
""")
create_table("""
CREATE TABLE customers (
    customer_id VARCHAR(64) PRIMARY KEY,
    club_member_status VARCHAR(50),
    fashion_news_frequency VARCHAR(50),
    age INT,
    postal_code VARCHAR(64)
)
""")

df_customers = pd.read_csv(customers_file, dtype=str)
df_customers = clean_df(df_customers)
df_customers['age'] = pd.to_numeric(df_customers['age'], errors='coerce')

cols = ",".join(f"[{c}]" for c in df_customers.columns if c in ['customer_id','club_member_status','fashion_news_frequency','age','postal_code'])
placeholders = ",".join("?" for _ in range(len(cols.split(","))))
sql_insert = f"INSERT INTO customers ({cols}) VALUES ({placeholders})"
cursor.executemany(sql_insert, [tuple(x) for x in df_customers[cols.split(",")].fillna('').values])
conn.commit()
print("✅ customers 寫入完成")

# =========================
# 3️⃣ transactions_train
# =========================
create_table("""
IF OBJECT_ID('transactions','U') IS NOT NULL DROP TABLE transactions
""")
create_table("""
CREATE TABLE transactions (
    transaction_id INT IDENTITY(1,1) PRIMARY KEY,
    t_dat DATE,
    customer_id VARCHAR(64),
    article_id VARCHAR(20),
    price DECIMAL(10,4),
    sales_channel_id INT
)
""")

chunksize = 100000
for chunk in pd.read_csv(transactions_file, dtype=str, chunksize=chunksize):
    chunk = clean_df(chunk)
    chunk['t_dat'] = pd.to_datetime(chunk['t_dat'], errors='coerce')
    chunk['price'] = pd.to_numeric(chunk['price'], errors='coerce')
    chunk['sales_channel_id'] = pd.to_numeric(chunk['sales_channel_id'], errors='coerce')

    cols = ",".join(chunk.columns)
    placeholders = ",".join("?" for _ in chunk.columns)
    sql_insert = f"INSERT INTO transactions ({cols}) VALUES ({placeholders})"
    cursor.executemany(sql_insert, [tuple(x) for x in chunk.fillna('').values])
    conn.commit()
    print(f"⏳ transactions chunk 完成")

# =========================
# 🔗 建 FK
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

print("🎉 三張表 ETL 完成！")