@app.get("/games")
async def get_games():
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={ODDS_API_KEY}&regions=us&markets=h2h"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client: # 延長超時等待到 30 秒
            response = await client.get(url)
            if response.status_code == 200:
                return {"data": response.json(), "status": "success"}
            else:
                return {"data": [], "status": "error", "message": "API 回應異常"}
    except Exception as e:
        return {"data": [], "status": "error", "message": str(e)}
