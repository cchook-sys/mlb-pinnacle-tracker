// 在 index.html 中加入以下邏輯來繪製趨勢圖
async function updateTrendChart(gameId) {
    const res = await fetch(`https://mlb-pinnacle-tracker.onrender.com/history/${gameId}`);
    const data = await res.json(); // 假設後端回傳 [ {ts: 10:00, price: -150}, {ts: 10:30, price: -160} ]
    
    const ctx = document.getElementById('myChart').getContext('2d');
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => d.ts),
            datasets: [{
                label: '水位趨勢',
                data: data.map(d => d.price),
                borderColor: '#38bdf8', // 專業藍
                tension: 0.3
            }]
        }
    });
}
