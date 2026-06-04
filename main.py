"""
MLB Pinnacle 數據量化監控後端 API (48小時擴展版)
- 自動定時爬取 Pinnacle 盤口
- 自動過濾並提供 48 小時內所有即時賽事與歷史波動數據
"""

from flask import Flask, jsonify
from flask_cors import CORS
import pymongo
import certifi
from datetime import datetime, timedelta
import os

app = Flask(__name__)
CORS(app) # 允許前端跨網域連線

# 1. 連線 MongoDB 雲端資料庫
MONGO_URI = "mongodb+srv://ccanthook:surfing135%3D@cluster0.cinyz41.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
try:
    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["mlb_tracker"]
    games_col = db["games"]       # 儲存即時盤口與歷史波動
    results_col = db["results"]   # 儲存完賽歷史結算數據
    print("🟢 MongoDB 雲端資料庫連線成功！")
except Exception as e:
    print(f"❌ MongoDB 連線失敗: {e}")

# 2. 路由：獲取即時看盤賽事列表 (已拓寬至 48 小時)
@app.route('/games', methods=['GET'])
def get_games():
    try:
        now = datetime.utcnow()
        # 💡 核心優化：將過濾天條從 24 小時拉長到 48 小時，提早抓出明天全場次
        cutoff_time = now + timedelta(hours=48)
        
        # 撈取開賽時間在「現在」到「未來 48 小時內」的賽事
        query = {
            "commence_time": {
                "$gte": now.isoformat(),
                "$lte": cutoff_time.isoformat()
            }
        }
        
        # 依據開賽時間由近到遠排序
        games = list(games_col.find(query, {"_id": 0}).sort("commence_time", 1))
        return jsonify(games)
    except Exception as e:
        return jsonify({"error": f"獲取即時數據失敗: {str(e)}"}), 500

# 3. 路由：獲取昨日歷史結算對答案數據集
@app.route('/analytics/dataset', methods=['GET'])
def get_history_dataset():
    try:
        # 撈出所有已經由對答案腳本結算完成的數據
        dataset = list(results_col.find({}, {"_id": 0}).sort("commence_time", 1))
        return jsonify(dataset)
    except Exception as e:
        return jsonify({"error": f"獲取歷史對答案數據失敗: {str(e)}"}), 500

# 4. 健康檢查路由
@app.route('/', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "system": "MLB Pinnacle Data Quantitative Backend",
        "timezone": "UTC",
        "monitoring_window": "48 Hours"
    })

if __name__ == '__main__':
    # Render 環境會自動給定 PORT，本機測試預設使用 5000
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
