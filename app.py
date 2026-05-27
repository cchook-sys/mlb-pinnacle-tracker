import os
import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# ⚠️ 1. 這裡導入你原本寫好的爬蟲函式
# 假設你原本的爬蟲寫在 crawler.py 裡的 get_mlb_data()
# from crawler import get_mlb_data

app = Flask(__name__)
CORS(app)  # 允許前端網頁跨網域抓取資料

def format_crawler_data():
    """
    這個函式負責執行你的爬蟲，並把資料整理成前端 UI 看得懂的 JSON 格式
    """
    try:
        # 呼叫你原本的爬蟲，拿到原始資料
        # raw_data = get_mlb_data() 
        
        # 💡 這裡將你的原始資料對應到前端需要的欄位（以下為示範結構）
        formatted_data = {
            "status": "success",
            "update_time_et": datetime.datetime.now().strftime("%I:%M:%S %p"), # 美東時間
            "games_count": 15, # 總場次
            "summary": {
                "playable": 1,  # 可進場數
                "moving": 1,    # 移動中數
                "signals": 7    # 有訊號數
            },
            "matches": [
                {
                    "time": "1:10 PM",
                    "countdown": "13h 19m",
                    "teams": "STL @ CIN",
                    "status": "未確認 vs 未確認",
                    "total_line": "8.4",
                    "line_change": "-0.4",
                    "trend": [8.8, 8.6, 8.4],
                    "speed": "0.10/r",
                    "direction": "小分 UNDER",
                    "over_percentage": "44%",
                    "state": "持續觀察"
                }
                # 可以用 for 迴圈把所有比賽 append 進來...
            ]
        }
        return formatted_data
    except Exception as e:
        print(f"爬蟲執行失敗: {e}")
        return None

# 2. 設定前端點擊更新時呼叫的 API 路由
@app.route('/api/fetch_live_data', methods=['GET'])
def get_live_data():
    data = format_crawler_data()
    if data:
        return jsonify(data), 200
    else:
        return jsonify({"status": "error", "message": "無法取得盤口資料"}), 500

# 3. 讓 Render 可以綁定 Port 跑起來
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
