@app.get("/games")
async def get_games():
    # 測試用：直接回傳文字，跳過資料庫讀取
    return {"status": "test", "message": "database skipped"}
