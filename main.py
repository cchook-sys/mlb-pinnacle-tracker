# 1. 必須先從 fastapi 導入
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx

# 2. 必須先宣告 app
app = FastAPI()

# 3. 再設定 Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 4. 最後才可以使用 @app.get
@app.get("/games")
async def get_games():
    # 這裡放你的 API 邏輯
    return {"status": "ok"}
