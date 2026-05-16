"""全国大蒜价格实时监控系统"""
import sqlite3
import os
import re
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler
import requests as http_requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "garlic.db")
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

# ========== 爬虫（证监会之星 + 惠农网） ==========

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

# 省份前缀映射
PROVINCES = ["黑龙江","吉林","辽宁","内蒙古","北京","天津","河北","河南","山东","山西",
             "陕西","甘肃","宁夏","青海","新疆","西藏","四川","重庆","贵州","云南",
             "广西","广东","海南","湖南","湖北","江西","安徽","江苏","浙江","福建","上海"]

# 城市/区县→省份（市场名不以省份开头的情况）
CITY_PROVINCE = {
    "乐亭县": "河北", "邯郸市": "河北", "邯郸": "河北", "秦皇岛": "河北", "怀来县": "河北",
    "三河市": "河北", "唐山市": "河北", "石家庄": "河北",
    "太原市": "山西", "大同市": "山西", "新绛县": "山西", "长治市": "山西",
    "临汾市": "山西", "晋城市": "山西", "汾阳市": "山西", "阳泉": "山西", "运城": "山西",
    "鄂尔多斯": "内蒙古", "呼和浩特": "内蒙古", "赤峰": "内蒙古", "包头": "内蒙古", "内蒙": "内蒙古",
    "鞍山": "辽宁", "朝阳市": "辽宁", "白山市": "吉林", "长春": "吉林",
    "哈尔滨": "黑龙江", "鹤岗市": "黑龙江",
    "南京": "江苏", "宜兴市": "江苏", "徐州": "江苏", "苏州": "江苏",
    "无锡": "江苏", "联谊": "江苏", "凌家塘": "江苏",
    "义乌市": "浙江", "宁波": "浙江", "良渚": "浙江", "嘉善": "浙江",
    "金华": "浙江", "嘉兴": "浙江", "绍兴": "浙江", "杭州": "浙江",
    "温岭市": "浙江", "慈溪市": "浙江",
    "天长市": "安徽", "亳州": "安徽", "合肥": "安徽", "阜阳": "安徽",
    "淮北市": "安徽", "砀山": "安徽", "蚌埠": "安徽",
    "厦门": "福建", "福鼎市": "福建", "乐平": "江西", "九江": "江西", "南昌": "江西",
    "青岛市": "山东", "青岛": "山东", "喜地": "山东", "章丘": "山东", "德州": "山东",
    "淄博市": "山东", "滕州市": "山东", "威海": "山东", "宁津县": "山东",
    "滨州": "山东", "寿光": "山东", "金乡": "山东", "山东": "山东",
    "洛阳": "河南", "金牛": "河南", "新野县": "河南", "商丘市": "河南",
    "濮阳": "河南", "万邦": "河南", "内黄": "河南",
    "四季青": "湖北", "襄樊": "湖北", "黄商": "湖北", "鄂州市": "湖北",
    "浠水": "湖北", "潜江市": "湖北", "洪湖": "湖北", "武汉": "湖北", "两湖": "湖北",
    "邵阳市": "湖南", "常德": "湖南",
    "新柳邕": "广西", "南宁": "广西", "海口市": "海南", "凤翔": "海南",
    "双福": "重庆",
    "成都": "四川", "南充": "四川", "绵阳": "四川", "达州": "四川", "广安市": "四川",
    "遵义": "贵州", "贵阳": "贵州",
    "呈贡": "云南", "昆明": "云南", "王旗营": "云南", "云菜": "云南",
    "领峰": "西藏",
    "泾云": "陕西", "咸阳": "陕西", "朱雀": "陕西",
    "平凉": "甘肃", "庆阳": "甘肃", "武威": "甘肃", "金昌": "甘肃", "临夏": "甘肃",
    "定西": "甘肃", "天水": "甘肃", "武山": "甘肃", "陇西": "甘肃",
    "酒泉": "甘肃", "陇国源": "甘肃", "兰州": "甘肃",
    "四季鲜": "宁夏", "海吉星": "宁夏", "吴忠市": "宁夏",
    "喀什": "新疆", "乌鲁木齐": "新疆", "通汇": "新疆", "绿珠": "新疆",
    "兵团": "新疆", "克拉玛依": "新疆", "九鼎": "新疆",
    "中牟县": "河南", "杞县": "河南", "中牟": "河南", "开封市": "河南", "郑州市": "河南",
    "孝义市": "山西", "大连": "辽宁", "阜新市": "辽宁", "沈阳": "辽宁",
    "马鞍山市": "安徽", "北海果业": "安徽", "黄淮": "河南",
    "长沙": "湖南", "广州": "广东", "佛山": "广东",
    "龙门实业": "重庆", "师宗县": "云南",
    "青藏高原": "青海", "拉萨": "西藏",
    "中俄": "黑龙江", "牡丹江": "黑龙江",
}


def extract_province(place: str) -> str:
    for p in PROVINCES:
        if place.startswith(p):
            return p
    for city, prov in CITY_PROVINCE.items():
        if place.startswith(city):
            return prov
    return "其他"


def find_latest_national_report() -> str:
    """扫描 stockstar 找到最新全国大蒜价格日报 URL，粗扫+精扫两阶段。"""
    today = datetime.now().strftime("%Y%m%d")

    # 粗扫：步长 30，覆盖 5000~50000，快速定位 ID 区域
    best_area, best_area_count = 0, 0
    for id_num in range(5000, 50000, 30):
        url = f"https://futures.stockstar.com/IG{today}{id_num:08d}.shtml"
        try:
            resp = http_requests.get(url, headers=HEADERS, timeout=5)
            if resp.status_code != 200:
                continue
            resp.encoding = "gb2312"
            count = resp.text.count("批发市场")
            if count > best_area_count and ("大蒜" in resp.text or "蒜" in resp.text):
                best_area_count = count
                best_area = id_num
        except Exception:
            continue

    if best_area_count < 3:
        return ""

    # 精扫：在最佳区域 ±200 逐条扫描
    best_url, best_count = "", 0
    for id_num in range(max(1000, best_area - 200), best_area + 200):
        url = f"https://futures.stockstar.com/IG{today}{id_num:08d}.shtml"
        try:
            resp = http_requests.get(url, headers=HEADERS, timeout=5)
            if resp.status_code != 200:
                continue
            resp.encoding = "gb2312"
            count = resp.text.count("批发市场")
            if count > best_count and ("大蒜" in resp.text or "蒜" in resp.text):
                best_count = count
                best_url = url
        except Exception:
            continue

    if best_count >= 10:
        return best_url
    # 如果没有汇总报告，收集所有含蒜的单个市场报告
    return best_url


def fetch_prices_stockstar() -> list:
    """从证监会之星抓取全国大蒜批发市场价格。返回 [{time, variety, origin, price}, ...]。"""
    url = find_latest_national_report()
    if not url:
        return []
    try:
        resp = http_requests.get(url, headers=HEADERS, timeout=15)
        resp.encoding = "gb2312"
        soup = BeautifulSoup(resp.text, "html.parser")
        content_div = soup.select_one(".content, .article, #article, #content, .news-content")
        text = content_div.get_text() if content_div else soup.get_text()
    except Exception:
        return []

    lines = [l.strip() for l in text.split("\n") if l.strip()]
    data_line = ""
    for line in lines:
        if "市场" in line and "价格" in line and "批发市场" in line and "数据来源" in line:
            data_line = line
            break
    if not data_line:
        return []

    cleaned = data_line.replace("市场 价格   ", "").replace("市场 价格    ", "")
    cleaned = re.sub(r"\s*单位：元/公斤.*$", "", cleaned)
    matches = re.findall(r"([一-鿿（）()·　Ⅰ-Ⅻ]+?)\s+(\d+\.?\d*)", cleaned)

    # 提取报告日期
    date_match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", lines[0] if lines else "")
    if date_match:
        report_date = f"{date_match.group(1)}-{date_match.group(2).zfill(2)}-{date_match.group(3).zfill(2)}"
    else:
        report_date = datetime.now().strftime("%Y-%m-%d")

    results = []
    for market, price_str in matches:
        price = float(price_str)
        price_jin = round(price / 2, 2)  # 元/公斤 → 元/斤
        province = extract_province(market)
        results.append({
            "time": report_date,
            "variety": "大蒜",
            "origin": market,
            "price": price_jin,
            "province": province,
        })
    return results


def fetch_prices_cnhnb() -> list:
    """从惠农网抓取大蒜价格（辅助数据源）。"""
    results = []
    try:
        resp = http_requests.get("https://www.cnhnb.com/hangqing/dasuan/", headers=HEADERS, timeout=15)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("div.quotation-content-list li.market-list-item")
        for item in items:
            time_el = item.select_one("span.time")
            product_el = item.select_one("span.product")
            place_el = item.select_one("span.place")
            price_el = item.select_one("span.price")
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
            })
    except Exception:
        pass
    return results


def save_prices(data: list, source: str):
    """将价格数据写入数据库，按(日期, 品种, 产地)去重。"""
    if not data:
        return
    now_ts = datetime.now().strftime("%H:%M:%S")
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
                    (f"{report_date} {now_ts}",
                     item["variety"], item["origin"], item["price"], source)
                )
        conn.commit()

# ========== FastAPI app ==========

@asynccontextmanager
async def lifespan(application: FastAPI):
    init_db()
    # 后台执行首次抓取，不阻塞服务启动
    threading.Thread(target=crawl_and_save, daemon=True).start()
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        lambda: crawl_and_save(),
        "interval",
        seconds=CRAWL_INTERVAL,
        id="crawler",
    )
    scheduler.start()
    yield
    scheduler.shutdown()


def crawl_and_save():
    stockstar_data = fetch_prices_stockstar()
    if stockstar_data:
        save_prices(stockstar_data, "证监会之星")
    cnhnb_data = fetch_prices_cnhnb()
    if cnhnb_data:
        save_prices(cnhnb_data, "惠农网")

app = FastAPI(title="全国大蒜价格监控", lifespan=lifespan)

# ========== API 接口 ==========

@app.get("/api/refresh")
def api_refresh():
    """手动刷新数据"""
    try:
        crawl_and_save()
        with get_db() as conn:
            last = conn.execute("SELECT MAX(time) as t FROM prices").fetchone()
            total = conn.execute("SELECT COUNT(*) as c FROM prices").fetchone()
        return {
            "success": True,
            "last_update": last["t"] if last else None,
            "total_records": total["c"] if total else 0,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/status")
def api_status():
    with get_db() as conn:
        last = conn.execute("SELECT MAX(time) as t FROM prices").fetchone()
        total = conn.execute("SELECT COUNT(*) as c FROM prices").fetchone()
    return {
        "last_update": last["t"],
        "data_date": last["t"][:10] if last and last["t"] else None,
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

@app.get("/api/provinces")
def api_provinces(province: str = Query(None), origin: str = Query(None)):
    """各省份大蒜价格明细（逐条记录，按价格降序），支持省份/产地筛选，含涨跌"""
    with get_db() as conn:
        latest_date = conn.execute("SELECT MAX(time) as t FROM prices").fetchone()
        if not latest_date or not latest_date["t"]:
            return {"data": [], "provinces": [], "origins": []}
        latest_day = latest_date["t"][:10]

        rows = conn.execute(
            "SELECT time, variety, origin, price, source FROM prices WHERE time LIKE ? ORDER BY price DESC",
            (f"{latest_day}%",)
        ).fetchall()

        # 批量查前一天价格做涨跌对比
        origins_set = list(set(r["origin"] for r in rows))
        prev_prices = {}
        if origins_set:
            prev_day_row = conn.execute(
                "SELECT MAX(time) FROM prices WHERE time < ?",
                (f"{latest_day} 00:00:00",)
            ).fetchone()
            if prev_day_row and prev_day_row[0]:
                prev_day = prev_day_row[0][:10]
                prev_rows = conn.execute(
                    "SELECT origin, price FROM prices WHERE time LIKE ?",
                    (f"{prev_day}%",)
                ).fetchall()
                prev_prices = {r["origin"]: r["price"] for r in prev_rows}

    result = []
    all_provinces = set()
    all_origins = set()
    for r in rows:
        prov = extract_province(r["origin"])
        all_provinces.add(prov)
        all_origins.add(r["origin"])

        if province and prov != province:
            continue
        if origin and origin not in r["origin"]:
            continue

        prev_price = prev_prices.get(r["origin"])
        change = round(r["price"] - prev_price, 2) if prev_price is not None else None
        change_pct = round(change / prev_price * 100, 2) if prev_price and prev_price != 0 else None

        result.append({
            "province": prov,
            "time": r["time"],
            "variety": r["variety"],
            "origin": r["origin"],
            "price": r["price"],
            "prev_price": prev_price,
            "change": change,
            "change_pct": change_pct,
            "source": r["source"] or "",
        })
    return {
        "data": result,
        "provinces": sorted(all_provinces),
        "origins": sorted(all_origins),
    }

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
        .topbar .title { font-size: 20px; font-weight: 700; color: #fff; white-space: nowrap; }
        .topbar .status { display: flex; align-items: center; gap: 12px; font-size: 13px; color: #94a3b8; flex-wrap: wrap; }
        .status-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
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
        .chart-controls button { padding: 6px 16px; border-radius: 6px; border: 1px solid #2a3a5c; background: #1a1a2e; color: #e0e0e0; cursor: pointer; font-size: 13px; white-space: nowrap; }
        .chart-controls button.active { background: #4ade80; color: #000; border-color: #4ade80; }
        .chart-controls button.variety { background: #2a3a5c; }
        .chart-controls button.variety.on { background: #3b82f6; border-color: #3b82f6; }
        .chart-wrap { position: relative; height: 320px; }
        canvas { width: 100% !important; height: 100% !important; }
        .table-section { background: #16213e; border-radius: 10px; padding: 20px; margin-bottom: 20px; border: 1px solid #2a3a5c; }
        .table-section h3 { margin-bottom: 12px; color: #fff; }
        .table-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }
        table { width: 100%; border-collapse: collapse; font-size: 13px; min-width: 600px; }
        th { color: #94a3b8; text-align: left; padding: 10px 12px; border-bottom: 1px solid #2a3a5c; white-space: nowrap; }
        td { padding: 10px 12px; border-bottom: 1px solid #1e293b; white-space: nowrap; }
        .up { color: #f87171; }
        .down { color: #4ade80; }
        .filter-row { display: flex; gap: 12px; margin-bottom: 14px; flex-wrap: wrap; align-items: center; }
        .btn-refresh { background: #3b82f6; color: #fff; border: none; padding: 8px 18px; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 600; display: flex; align-items: center; gap: 6px; transition: background 0.2s; }
        .btn-refresh:hover { background: #2563eb; }
        .btn-refresh:active { transform: scale(0.95); }
        .btn-refresh.loading { background: #64748b; pointer-events: none; }
        .btn-refresh .icon { display: inline-block; font-size: 16px; transition: transform 0.3s; }
        .btn-refresh.loading .icon { animation: spin 0.8s linear infinite; }
        @keyframes spin { 100% { transform: rotate(360deg); } }
        .refresh-status { font-size: 11px; color: #94a3b8; margin-left: 4px; }

        /* ====== 平板 ====== */
        @media (max-width: 768px) {
            body { padding: 10px; }
            .topbar { flex-direction: column; align-items: flex-start; padding: 12px 16px; gap: 8px; }
            .topbar .title { font-size: 17px; }
            .topbar .status { font-size: 11px; gap: 6px; }
            .btn-refresh { padding: 6px 14px; font-size: 12px; }
            .cards { grid-template-columns: repeat(3, 1fr); gap: 8px; }
            .card { padding: 14px 6px; }
            .card .value { font-size: 26px; }
            .card .label { font-size: 11px; margin-bottom: 4px; }
            .card .unit { font-size: 10px; }
            .card .detail { font-size: 11px; margin-top: 4px; }
            .table-section { padding: 12px; }
            .table-section h3 { font-size: 14px; }
            table { font-size: 11px; min-width: 550px; }
            th, td { padding: 7px 8px; }
            .chart-wrap { height: 240px; }
            .chart-section { padding: 12px; }
            .chart-controls { gap: 4px; }
            .chart-controls button { padding: 5px 10px; font-size: 11px; }
            .filter-row { flex-direction: column; align-items: stretch; gap: 8px; }
            .filter-row input { width: 100% !important; }
        }

        /* ====== 小屏手机 ====== */
        @media (max-width: 480px) {
            .cards { grid-template-columns: repeat(3, 1fr); gap: 6px; }
            .card { padding: 10px 3px; }
            .card .value { font-size: 20px; }
            .card .label { font-size: 10px; }
            .card .detail { font-size: 10px; }
            .topbar .title { font-size: 15px; }
            .topbar .status { font-size: 10px; }
            .chart-wrap { height: 200px; }
            table { font-size: 10px; min-width: 480px; }
            th, td { padding: 5px 6px; }
        }
    </style>
</head>
<body>
    <div class="topbar">
        <div class="title">全国大蒜价格监控</div>
        <div class="status">
            <span id="updateTime">加载中...</span>
            <span class="status-dot ok" id="statusDot"></span>
            <span id="statusText">正常</span>
            <button class="btn-refresh" id="btnRefresh" onclick="manualRefresh()" title="手动刷新最新数据">
                <span class="icon">&#x21bb;</span> 刷新数据
            </button>
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

    <div class="province-section table-section">
        <h3>各省份价格明细（按价格降序）</h3>
        <div class="filter-row">
            <label style="font-size:13px;color:#94a3b8;">省份:</label>
            <select id="provinceFilter" style="background:#1a1a2e;color:#e0e0e0;border:1px solid #2a3a5c;padding:6px 10px;border-radius:6px;font-size:13px;">
                <option value="">全部</option>
            </select>
            <label style="font-size:13px;color:#94a3b8;margin-left:8px;">产地搜索:</label>
            <input id="originFilter" type="text" placeholder="输入市场名称..." style="background:#1a1a2e;color:#e0e0e0;border:1px solid #2a3a5c;padding:6px 10px;border-radius:6px;font-size:13px;width:200px;">
            <span id="filterCount" style="font-size:12px;color:#64748b;margin-left:8px;"></span>
        </div>
        <div class="table-wrap">
        <table>
            <thead><tr><th>省份</th><th>品种</th><th>产地</th><th>价格(元/斤)</th><th>涨跌</th><th>涨幅</th><th>更新时间</th></tr></thead>
            <tbody id="provinceTableBody"></tbody>
        </table>
        </div>
    </div>

    <div class="chart-section">
        <div class="chart-controls">
            <button class="active" data-range="24">24小时</button>
            <button data-range="168">7天</button>
            <button data-range="720">30天</button>
            <span style="flex:1;"></span>
            <span style="font-size:13px;color:#94a3b8;">全国均价趋势</span>
        </div>
        <div class="chart-wrap"><canvas id="chart"></canvas></div>
    </div>

    <div class="table-section">
        <h3>最新行情记录</h3>
        <div class="table-wrap">
        <table>
            <thead><tr><th>时间</th><th>品种</th><th>产地</th><th>省份</th><th>价格(元/斤)</th><th>涨跌</th><th>来源</th></tr></thead>
            <tbody id="tableBody"></tbody>
        </table>
        </div>
    </div>

    <script>
        let chart;
        let currentRange = 24;

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
                var prov = document.getElementById('provinceFilter').value || '';
                var orig = document.getElementById('originFilter').value || '';
                var qs = [];
                if (prov) qs.push('province=' + encodeURIComponent(prov));
                if (orig) qs.push('origin=' + encodeURIComponent(orig));
                var provUrl = '/api/provinces' + (qs.length ? '?' + qs.join('&') : '');

                var results = await Promise.all([
                    fetch('/api/price'),
                    fetch('/api/status'),
                    fetch(provUrl)
                ]);
                var priceData = await results[0].json();
                var statusData = await results[1].json();
                var provinceResp = await results[2].json();
                updateStatus(statusData);
                updateCards(priceData);
                renderProvinces(provinceResp);
                if (!chart) loadHistory(currentRange);
            } catch(e) {
                document.getElementById('statusText').textContent = '连接失败';
                document.getElementById('statusDot').className = 'status-dot error';
            }
        }

        function renderProvinces(resp) {
            var data = resp.data || [];
            var tbody = document.getElementById('provinceTableBody');
            var countEl = document.getElementById('filterCount');
            if (!data || data.length === 0) {
                tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#94a3b8;">暂无数据</td></tr>';
                countEl.textContent = '';
                return;
            }
            countEl.textContent = '共 ' + data.length + ' 条';
            tbody.innerHTML = data.map(function(r) {
                var changeHtml = '<span style="color:#64748b;">-</span>';
                var pctHtml = '<span style="color:#64748b;">-</span>';
                if (r.change !== null && r.change !== undefined) {
                    var dir = r.change > 0 ? '↑' : r.change < 0 ? '↓' : '';
                    var cls = r.change > 0 ? 'up' : r.change < 0 ? 'down' : '';
                    changeHtml = '<span class="' + cls + '">' + dir + ' ' + Math.abs(r.change).toFixed(2) + '</span>';
                    pctHtml = '<span class="' + cls + '">' + dir + ' ' + Math.abs(r.change_pct).toFixed(1) + '%</span>';
                }
                var priceColor = r.price >= 5 ? 'color:#f87171;' : r.price <= 2 ? 'color:#4ade80;' : '';
                return '<tr>' +
                    '<td><span style="background:#2a3a5c;padding:2px 8px;border-radius:4px;font-size:11px;">' + r.province + '</span></td>' +
                    '<td>' + r.variety + '</td>' +
                    '<td>' + r.origin + '</td>' +
                    '<td><strong style="' + priceColor + '">' + r.price.toFixed(2) + '</strong></td>' +
                    '<td>' + changeHtml + '</td>' +
                    '<td>' + pctHtml + '</td>' +
                    '<td style="color:#64748b;font-size:12px;">' + r.time + '</td>' +
                    '</tr>';
            }).join('');

            // 填充省份下拉
            var sel = document.getElementById('provinceFilter');
            var currentVal = sel.value;
            var provinces = resp.provinces || [];
            sel.innerHTML = '<option value="">全部</option>' +
                provinces.map(function(p) {
                    return '<option value="' + p + '">' + p + '</option>';
                }).join('');
            if (currentVal) sel.value = currentVal;

            // 同时更新底部行情记录表（显示最近50条）
            var recData = data.slice(0, 50);
            var recTbody = document.getElementById('tableBody');
            recTbody.innerHTML = recData.map(function(r) {
                var changeHtml = '<span style="color:#64748b;">-</span>';
                if (r.change !== null && r.change !== undefined) {
                    var dir = r.change > 0 ? '↑' : r.change < 0 ? '↓' : '';
                    var cls = r.change > 0 ? 'up' : r.change < 0 ? 'down' : '';
                    changeHtml = '<span class="' + cls + '">' + dir + ' ' + Math.abs(r.change).toFixed(2) + ' 元</span>';
                }
                return '<tr>' +
                    '<td style="font-size:12px;color:#94a3b8;">' + r.time + '</td>' +
                    '<td>' + r.variety + '</td>' +
                    '<td>' + r.origin + '</td>' +
                    '<td><span style="background:#2a3a5c;padding:2px 8px;border-radius:4px;font-size:11px;">' + r.province + '</span></td>' +
                    '<td><strong>' + r.price.toFixed(2) + '</strong></td>' +
                    '<td>' + changeHtml + '</td>' +
                    '<td style="color:#64748b;">' + (r.source || '') + '</td>' +
                    '</tr>';
            }).join('');
        }

        document.getElementById('provinceFilter').addEventListener('change', function() { refresh(); });
        var originTimer = null;
        document.getElementById('originFilter').addEventListener('input', function() {
            clearTimeout(originTimer);
            originTimer = setTimeout(function() { refresh(); }, 300);
        });

        function updateStatus(data) {
            var el = document.getElementById('updateTime');
            var dot = document.getElementById('statusDot');
            var txt = document.getElementById('statusText');
            if (data.last_update) {
                el.textContent = '最后更新: ' + data.last_update;

                var dataDate = data.last_update.substring(0, 10);
                var today = new Date().toISOString().substring(0, 10);
                var yesterday = new Date(Date.now() - 86400000).toISOString().substring(0, 10);

                if (dataDate === today) {
                    dot.className = 'status-dot ok';
                    txt.textContent = '今日数据';
                    txt.style.color = '#4ade80';
                } else if (dataDate === yesterday) {
                    dot.className = 'status-dot ok';
                    txt.textContent = '昨日数据';
                    txt.style.color = '#fbbf24';
                } else {
                    dot.className = 'status-dot error';
                    txt.textContent = '数据过期 (' + dataDate + ')';
                    txt.style.color = '#f87171';
                }
            } else {
                el.textContent = '暂无数据';
                dot.className = 'status-dot error';
                txt.textContent = '数据缺失';
                txt.style.color = '#f87171';
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

            // 按时段分组计算全国均价
            var buckets = {};
            data.forEach(function(d) {
                // 按小时聚合
                var key = d.time.substring(0, 13); // "2026-05-15 18"
                if (!buckets[key]) buckets[key] = { sum: 0, count: 0 };
                buckets[key].sum += d.price;
                buckets[key].count += 1;
            });
            var avgPoints = Object.keys(buckets).sort().map(function(k) {
                return { x: k + ':00', y: parseFloat((buckets[k].sum / buckets[k].count).toFixed(2)) };
            });

            var datasets = [{
                label: '全国均价',
                data: avgPoints,
                borderColor: '#4ade80',
                backgroundColor: 'rgba(74,222,128,0.1)',
                fill: true,
                tension: 0.3,
                pointRadius: 0,
                borderWidth: 2,
            }];

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

        async function manualRefresh() {
            var btn = document.getElementById('btnRefresh');
            btn.classList.add('loading');
            btn.innerHTML = '<span class="icon">&#x21bb;</span> 刷新中...';
            try {
                var resp = await fetch('/api/refresh');
                var result = await resp.json();
                if (result.success) {
                    await refresh();
                }
            } catch(e) {
                console.error('Refresh failed:', e);
            } finally {
                btn.classList.remove('loading');
                btn.innerHTML = '<span class="icon">&#x21bb;</span> 刷新数据';
            }
        }
    </script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
def homepage():
    return DASHBOARD_HTML
