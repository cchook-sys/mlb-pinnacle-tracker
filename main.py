@app.on_event("startup")
async def startup_event():
    print(">>> 系統啟動：開始初始化背景任務...")
    asyncio.create_task(fetch_odds_task())
    print(">>> 背景任務已排入排程。")
