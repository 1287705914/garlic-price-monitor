"""全国大蒜价格实时监控系统"""
import sqlite3
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler
import requests as http_requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "garlic.db")
CRAWL_URL = os.getenv(
    "CRAWL_URL",
    "https://www.cnhnb.com/hangqing/dasuan/"
)
CRAWL_PAGES = int(os.getenv("CRAWL_PAGES", "3"))
CRAWL_INTERVAL = int(os.getenv("CRAWL_INTERVAL", "3600"))

# ========== 数据库 ==========

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time DATETIME DEFAULT CURRENT_TIMESTAMP,
                variety TEXT NOT NULL,
                origin TEXT NOT NULL,
                price REAL NOT NULL,
                source TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_time ON prices(time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_variety_origin ON prices(variety, origin)")
        conn.commit()

# ========== 爬虫 ==========

def fetch_prices() -> list:
    """从惠农网抓取全国大蒜价格数据。返回 [{time, variety, origin, price, change}, ...]。"""
    results = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    url = CRAWL_URL
    try:
        resp = http_requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("div.quotation-content-list li.market-list-item")
        for item in items:
            time_el = item.select_one("span.time")
            product_el = item.select_one("span.product")
            place_el = item.select_one("span.place")
            price_el = item.select_one("span.price")
            change_el = item.select_one("span.lifting")
            if not all([time_el, product_el, place_el, price_el]):
                continue
            product = product_el.get_text(strip=True)
            place = place_el.get_text(strip=True)
            price_text = price_el.get_text(strip=True)
            if "蒜" not in product:
                continue
            try:
                price = float(price_text.replace("元/斤", "").replace("元/公斤", "").strip())
            except ValueError:
                continue
            results.append({
                "time": time_el.get_text(strip=True),
                "variety": product,
                "origin": place,
                "price": price,
                "change": change_el.get_text(strip=True) if change_el else "-",
            })
    except Exception:
        pass
    return results

def save_prices(data: list, source: str):
    """将价格数据写入数据库，按(日期, 品种, 产地)去重。"""
    if not data:
        return
    with get_db() as conn:
        for item in data:
            report_date = item.get("time", datetime.now().strftime("%Y-%m-%d"))
            existing = conn.execute(
                "SELECT id FROM prices WHERE time LIKE ? AND variety=? AND origin=?",
                (f"{report_date}%", item["variety"], item["origin"])
            ).fetchone()
            if not existing:
                conn.execute(
                    "INSERT INTO prices (time, variety, origin, price, source) VALUES (?, ?, ?, ?, ?)",
                    (f"{report_date} {datetime.now().strftime('%H:%M:%S')}",
                     item["variety"], item["origin"], item["price"], source)
                )
        conn.commit()

# ========== FastAPI app ==========

@asynccontextmanager
async def lifespan(application: FastAPI):
    init_db()
    # 启动时立即抓一次
    save_prices(fetch_prices(), "惠农网")
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        lambda: save_prices(fetch_prices(), "惠农网"),
        "interval",
        seconds=CRAWL_INTERVAL,
        id="crawler",
    )
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(title="全国大蒜价格监控", lifespan=lifespan)

# ========== API 接口 ==========

@app.get("/api/status")
def api_status():
    with get_db() as conn:
        last = conn.execute("SELECT MAX(time) as t FROM prices").fetchone()
        total = conn.execute("SELECT COUNT(*) as c FROM prices").fetchone()
    return {
        "last_update": last["t"],
        "total_records": total["c"],
    }

@app.get("/api/price")
def api_price(variety: str = Query(None), origin: str = Query(None)):
    with get_db() as conn:
        latest = conn.execute("SELECT MAX(time) as t FROM prices").fetchone()
        if not latest or not latest["t"]:
            return {"current": None, "today_high": None, "today_low": None, "change": None}
        latest_day = latest["t"][:10]
        where = "WHERE time LIKE ?"
        params = [f"{latest_day}%"]
        if variety:
            where += " AND variety=?"
            params.append(variety)
        if origin:
            where += " AND origin=?"
            params.append(origin)

        row = conn.execute(
            f"SELECT price FROM prices {where} ORDER BY time DESC LIMIT 1", params
        ).fetchone()
        high_row = conn.execute(
            f"SELECT MAX(price) as v, variety, origin FROM prices {where}", params
        ).fetchone()
        low_row = conn.execute(
            f"SELECT MIN(price) as v, variety, origin FROM prices {where}", params
        ).fetchone()
        prev = conn.execute(
            f"SELECT price FROM prices {where} ORDER BY time DESC LIMIT 1 OFFSET 1", params
        ).fetchone()

    current = row["price"] if row else None
    change = None
    if current and prev and prev["price"]:
        change = round(current - prev["price"], 2)

    return {
        "current": current,
        "today_high": {
            "price": high_row["v"], "variety": high_row["variety"], "origin": high_row["origin"]
        } if high_row and high_row["v"] else None,
        "today_low": {
            "price": low_row["v"], "variety": low_row["variety"], "origin": low_row["origin"]
        } if low_row and low_row["v"] else None,
        "change": change,
    }

@app.get("/api/history")
def api_history(hours: int = Query(None), days: int = Query(None)):
    with get_db() as conn:
        if days:
            since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        elif hours:
            since = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        else:
            since = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")

        rows = conn.execute(
            "SELECT time, variety, origin, price FROM prices WHERE time >= ? ORDER BY time ASC",
            (since,)
        ).fetchall()

    return [{"time": r["time"], "variety": r["variety"],
             "origin": r["origin"], "price": r["price"]} for r in rows]

# ========== 前端 Dashboard ==========

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>全国大蒜价格监控</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: #0f0f1a; color: #e0e0e0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 20px; }
        .topbar { display: flex; align-items: center; justify-content: space-between; background: #1a1a2e; padding: 16px 24px; border-radius: 10px; margin-bottom: 20px; flex-wrap: wrap; gap: 10px; }
        .topbar .title { font-size: 20px; font-weight: 700; color: #fff; }
        .topbar .status { display: flex; align-items: center; gap: 12px; font-size: 13px; color: #94a3b8; }
        .status-dot { width: 10px; height: 10px; border-radius: 50%; }
        .status-dot.ok { background: #4ade80; }
        .status-dot.error { background: #f87171; animation: blink 0.5s infinite; }
        @keyframes blink { 50% { opacity: 0.3; } }
        .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 20px; }
        .card { background: #16213e; border-radius: 10px; padding: 24px; text-align: center; border: 1px solid #2a3a5c; }
        .card .label { font-size: 13px; color: #94a3b8; margin-bottom: 8px; }
        .card .value { font-size: 42px; font-weight: 800; color: #fff; }
        .card .unit { font-size: 13px; color: #94a3b8; }
        .card .detail { margin-top: 8px; font-size: 14px; }
        .card .detail.up { color: #f87171; }
        .card .detail.down { color: #4ade80; }
        .card.alert .value { color: #f87171; animation: blink 0.5s infinite; }
        .chart-section { background: #16213e; border-radius: 10px; padding: 20px; margin-bottom: 20px; border: 1px solid #2a3a5c; }
        .chart-controls { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; align-items: center; }
        .chart-controls button { padding: 6px 16px; border-radius: 6px; border: 1px solid #2a3a5c; background: #1a1a2e; color: #e0e0e0; cursor: pointer; font-size: 13px; }
        .chart-controls button.active { background: #4ade80; color: #000; border-color: #4ade80; }
        .chart-controls button.variety { background: #2a3a5c; }
        .chart-controls button.variety.on { background: #3b82f6; border-color: #3b82f6; }
        .chart-wrap { position: relative; height: 320px; }
        canvas { width: 100% !important; height: 100% !important; }
        .table-section { background: #16213e; border-radius: 10px; padding: 20px; border: 1px solid #2a3a5c; }
        .table-section h3 { margin-bottom: 12px; color: #fff; }
        table { width: 100%; border-collapse: collapse; font-size: 13px; }
        th { color: #94a3b8; text-align: left; padding: 10px 12px; border-bottom: 1px solid #2a3a5c; }
        td { padding: 10px 12px; border-bottom: 1px solid #1e293b; }
        .up { color: #f87171; }
        .down { color: #4ade80; }
    </style>
</head>
<body>
    <div class="topbar">
        <div class="title">全国大蒜价格监控</div>
        <div class="status">
            <span id="updateTime">加载中...</span>
            <span class="status-dot ok" id="statusDot"></span>
            <span id="statusText">正常</span>
            <span style="background:#2a3a5c;padding:4px 10px;border-radius:4px;">60s 刷新</span>
        </div>
    </div>

    <div class="cards">
        <div class="card" id="cardCurrent">
            <div class="label">当前均价</div>
            <div class="value" id="currentPrice">--</div>
            <div class="unit">元/斤</div>
            <div class="detail" id="currentChange" style="display:none;"></div>
        </div>
        <div class="card">
            <div class="label">今日最高</div>
            <div class="value" id="highPrice">--</div>
            <div class="unit">元/斤</div>
            <div class="detail" id="highDetail" style="color:#94a3b8;"></div>
        </div>
        <div class="card">
            <div class="label">今日最低</div>
            <div class="value" id="lowPrice">--</div>
            <div class="unit">元/斤</div>
            <div class="detail" id="lowDetail" style="color:#94a3b8;"></div>
        </div>
    </div>

    <div class="chart-section">
        <div class="chart-controls">
            <button class="active" data-range="24">24小时</button>
            <button data-range="168">7天</button>
            <button data-range="720">30天</button>
            <span style="flex:1;"></span>
            <span style="font-size:13px;color:#94a3b8;">品种:</span>
            <button class="variety on" data-var="白蒜">白蒜</button>
            <button class="variety on" data-var="红蒜">红蒜</button>
            <button class="variety on" data-var="杂交蒜">杂交蒜</button>
        </div>
        <div class="chart-wrap"><canvas id="chart"></canvas></div>
    </div>

    <div class="table-section">
        <h3>最新行情记录</h3>
        <table>
            <thead><tr><th>时间</th><th>品种</th><th>产地</th><th>价格(元/斤)</th><th>涨跌</th><th>来源</th></tr></thead>
            <tbody id="tableBody"></tbody>
        </table>
    </div>

    <script>
        let chart;
        const COLORS = { '白蒜': '#60a5fa', '红蒜': '#f472b6', '杂交蒜': '#fbbf24' };
        let selectedVarieties = ['白蒜', '红蒜', '杂交蒜'];
        let currentRange = 24;

        document.querySelectorAll('button.variety').forEach(function(btn) {
            btn.addEventListener('click', function() {
                var v = btn.dataset.var;
                if (selectedVarieties.includes(v)) {
                    if (selectedVarieties.length > 1) {
                        selectedVarieties = selectedVarieties.filter(function(x) { return x !== v; });
                        btn.classList.remove('on');
                    }
                } else {
                    selectedVarieties.push(v);
                    btn.classList.add('on');
                }
                loadHistory(currentRange);
            });
        });

        document.querySelectorAll('button[data-range]').forEach(function(btn) {
            btn.addEventListener('click', function() {
                document.querySelectorAll('button[data-range]').forEach(function(b) { b.classList.remove('active'); });
                btn.classList.add('active');
                currentRange = parseInt(btn.dataset.range);
                loadHistory(currentRange);
            });
        });

        async function refresh() {
            try {
                var results = await Promise.all([
                    fetch('/api/price'),
                    fetch('/api/status'),
                    fetch('/api/history?hours=1')
                ]);
                var priceData = await results[0].json();
                var statusData = await results[1].json();
                var tableData = await results[2].json();
                updateStatus(statusData);
                updateCards(priceData);
                updateTable(tableData);
                if (!chart) loadHistory(currentRange);
            } catch(e) {
                document.getElementById('statusText').textContent = '连接失败';
                document.getElementById('statusDot').className = 'status-dot error';
            }
        }

        function updateStatus(data) {
            var el = document.getElementById('updateTime');
            var dot = document.getElementById('statusDot');
            var txt = document.getElementById('statusText');
            if (data.last_update) {
                el.textContent = '最后更新: ' + data.last_update;
                dot.className = 'status-dot ok';
                txt.textContent = '正常';
            } else {
                el.textContent = '暂无数据';
                dot.className = 'status-dot error';
                txt.textContent = '数据缺失';
            }
        }

        function updateCards(data) {
            document.getElementById('currentPrice').textContent = data.current != null ? data.current : '--';
            document.getElementById('highPrice').textContent = (data.today_high && data.today_high.price) != null ? data.today_high.price : '--';
            document.getElementById('lowPrice').textContent = (data.today_low && data.today_low.price) != null ? data.today_low.price : '--';

            if (data.today_high) {
                document.getElementById('highDetail').textContent =
                    data.today_high.origin + ' · ' + data.today_high.variety;
            }
            if (data.today_low) {
                document.getElementById('lowDetail').textContent =
                    data.today_low.origin + ' · ' + data.today_low.variety;
            }

            var changeEl = document.getElementById('currentChange');
            var card = document.getElementById('cardCurrent');
            if (data.change && data.change !== 0 && data.current) {
                changeEl.style.display = 'block';
                var dir = data.change > 0 ? '↑' : '↓';
                var cls = data.change > 0 ? 'up' : 'down';
                changeEl.className = 'detail ' + cls;
                var prevPrice = data.current - data.change;
                var pct = prevPrice !== 0 ? (data.change / prevPrice * 100).toFixed(2) : '0.00';
                changeEl.textContent = dir + ' ' + Math.abs(data.change).toFixed(2) + ' (' + pct + '%)';

                if (Math.abs(parseFloat(pct)) > 10) {
                    card.classList.add('alert');
                } else {
                    card.classList.remove('alert');
                }
            } else {
                changeEl.style.display = 'none';
                card.classList.remove('alert');
            }
        }

        async function loadHistory(hours) {
            var resp = await fetch('/api/history?hours=' + hours);
            var data = await resp.json();

            var datasets = [];
            for (var i = 0; i < selectedVarieties.length; i++) {
                var v = selectedVarieties[i];
                var points = data.filter(function(d) { return d.variety === v; });
                datasets.push({
                    label: v,
                    data: points.map(function(p) { return { x: p.time, y: p.price }; }),
                    borderColor: COLORS[v] || '#fff',
                    backgroundColor: 'transparent',
                    tension: 0.3,
                    pointRadius: 0,
                    borderWidth: 2,
                });
            }

            var ctx = document.getElementById('chart').getContext('2d');
            if (chart) chart.destroy();
            chart = new Chart(ctx, {
                type: 'line',
                data: { datasets: datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: {
                            ticks: { color: '#94a3b8', maxTicksLimit: 12 },
                            grid: { color: '#1e293b' }
                        },
                        y: {
                            ticks: { color: '#94a3b8', callback: function(v) { return v + ' 元'; } },
                            grid: { color: '#1e293b' }
                        }
                    },
                    plugins: { legend: { labels: { color: '#e0e0e0' } } },
                    interaction: { intersect: false, mode: 'index' }
                }
            });
        }

        function updateTable(data) {
            var tbody = document.getElementById('tableBody');
            var rows = data.slice().reverse().slice(0, 50);
            var prevPrice = null;
            tbody.innerHTML = rows.map(function(r) {
                var changeHtml = '<span style="color:#64748b;">-</span>';
                if (prevPrice !== null && r.price !== prevPrice) {
                    var cls = r.price > prevPrice ? 'up' : 'down';
                    var arrow = r.price > prevPrice ? '↑' : '↓';
                    changeHtml = '<span class="' + cls + '">' + arrow + ' ' + Math.abs(r.price - prevPrice).toFixed(2) + '</span>';
                }
                prevPrice = r.price;
                return '<tr>' +
                    '<td>' + r.time + '</td>' +
                    '<td>' + r.variety + '</td>' +
                    '<td>' + r.origin + '</td>' +
                    '<td>' + r.price + '</td>' +
                    '<td>' + changeHtml + '</td>' +
                    '<td style="color:#64748b;">惠农网</td>' +
                    '</tr>';
            }).join('');
        }

        refresh();
        setInterval(refresh, 60000);
    </script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
def homepage():
    return DASHBOARD_HTML
