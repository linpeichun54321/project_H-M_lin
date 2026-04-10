# =========================
# 🟢 完整 Python 專案啟動流程
# =========================

# 1️⃣ 進入專案資料夾
#cd C:\projects\project_H-M_lin

# 2️⃣ 關閉其他虛擬環境（如果有）
#deactivate

# 3️⃣ 建立專案虛擬環境（如果已存在，可跳過）
#C:\Users\linpeichunhappy\AppData\Local\Programs\Python\Python312\python.exe -m venv venv

# 4️⃣ 啟動虛擬環境
#.\venv\Scripts\Activate.ps1

# 確認目前 Python 版本與路徑
#python --version
#where python
# 確認路徑應該是 C:\projects\project_H-M_lin\venv\Scripts\python.exe

# 5️⃣ 更新 pip（避免版本太舊）
#python -m pip install --upgrade pip

# 6️⃣ 安裝必要套件
#pip install numpy==1.26.4 pandas pyodbc

# 7️⃣ 確認套件安裝成功
#python -c "import pandas as pd; import pyodbc; print('套件測試通過 ✅')"

# 8️⃣ 執行程式
#python main.py

# =========================
# 💡 注意：
# - 不要在 Python 互動環境 (>>>) 裡執行 pip install 或 python -c
# - 每次打開新 PowerShell，都要先啟動虛擬環境
# - 之後新增套件可用 pip install 套件名
# - 可用 pip freeze > requirements.txt 生成需求檔，方便複製給別人
# =========================