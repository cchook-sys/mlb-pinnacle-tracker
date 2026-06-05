from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# 允許跨域請求，這是前端能正確讀取數據的關鍵
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "API is online", "docs": "/docs"}

@app.get("/games")
async def get_games():
    # 這裡放你原本抓取 MLB 數據的邏輯
    # 為了測試，我們先回傳一個簡單的 JSON
    return {"data": "API is working, please fetch /games for data"}
