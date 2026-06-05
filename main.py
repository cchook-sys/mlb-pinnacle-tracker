@app.get("/games")
async def get_games():
    # 改用更寬鬆的 API 請求，並移除複雜的過濾邏輯
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey=79112bb70773a2cdf998cb3112b18589&regions=us&markets=h2h,totals&oddsFormat=american"
    async with httpx.AsyncClient(timeout=20) as client:
        res = await client.get(url)
        data = res.json()
        return {
            "system_updated_at": datetime.now().strftime("%H:%M:%S"),
            "data": data  # 這裡直接回傳 API 原始數據
        }
