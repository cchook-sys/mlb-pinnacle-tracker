@app.get("/games")
async def get_games():
    # 簡化參數，先確保能撈到任何東西
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h"
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(url)
        # 直接回傳原始回傳內容，看看是不是金鑰過期或是其他錯誤
        return {"raw_data": response.json(), "status_code": response.status_code}
