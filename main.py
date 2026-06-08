from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# 設定 CORS，確保前端 GitHub Pages 可以存取
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 定義根目錄，解決 404
@app.get("/")
def read_root():
    return {"status": "ok", "message": "API is running"}

# 定義 games 路由
@app.get("/games")
def get_games():
    # 暫時回傳測試資料，確保路由通暢
    return {"status": "ok", "data": []}
