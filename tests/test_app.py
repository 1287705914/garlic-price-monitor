import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import tempfile
from datetime import datetime, timedelta
from fastapi.testclient import TestClient
import app as app_module


@pytest.fixture
def test_db():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp_path = tmp.name
    tmp.close()
    old_path = app_module.DB_PATH
    app_module.DB_PATH = tmp_path
    app_module.init_db()
    yield tmp_path
    app_module.DB_PATH = old_path
    try:
        os.unlink(tmp_path)
    except PermissionError:
        pass


@pytest.fixture
def client(test_db):
    conn = app_module.get_db()
    now = datetime.now()
    for i in range(10):
        conn.execute(
            "INSERT INTO prices (time, variety, origin, price, source) VALUES (?, ?, ?, ?, ?)",
            ((now - timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S"),
             "白蒜" if i % 2 == 0 else "红蒜",
             "中牟" if i < 5 else "杞县",
             3.80 + i * 0.02,
             "test")
        )
    conn.commit()
    conn.close()
    return TestClient(app_module.app)


class TestDatabase:
    def test_init_db_creates_table(self, test_db):
        conn = app_module.get_db()
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        assert any(t["name"] == "prices" for t in tables)

    def test_insert_and_query(self, test_db):
        conn = app_module.get_db()
        conn.execute(
            "INSERT INTO prices (time, variety, origin, price, source) VALUES (?, ?, ?, ?, ?)",
            ("2026-05-15 10:00:00", "白蒜", "中牟", 3.85, "test")
        )
        conn.commit()
        rows = conn.execute(
            "SELECT * FROM prices ORDER BY time DESC"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["variety"] == "白蒜"
        assert rows[0]["price"] == 3.85


class TestCrawler:
    def test_parse_cnhnb_html(self):
        """测试惠农网行情列表解析"""
        sample_html = """
        <html><body>
        <div class="quotation-content-list">
        <ul>
        <li class="market-list-item">
          <a href="/hangqing/cd-1/">
            <span class="time">2026-05-15</span>
            <span class="product">白蒜</span>
            <span class="place">郑州市中牟县</span>
            <span class="price">3.85元/斤</span>
            <span class="lifting">-0.05</span>
          </a>
        </li>
        <li class="market-list-item">
          <a href="/hangqing/cd-2/">
            <span class="time">2026-05-15</span>
            <span class="product">红蒜</span>
            <span class="place">开封市杞县</span>
            <span class="price">3.60元/斤</span>
            <span class="lifting">+0.10</span>
          </a>
        </li>
        <li class="market-list-item">
          <a href="/hangqing/cd-3/">
            <span class="time">2026-05-15</span>
            <span class="product">杂交蒜</span>
            <span class="place">山东潍坊市</span>
            <span class="price">2.80元/斤</span>
            <span class="lifting">-</span>
          </a>
        </li>
        </ul>
        </div>
        </body></html>"""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(sample_html, "html.parser")
        items = soup.select("div.quotation-content-list li.market-list-item")

        results = []
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
            price = float(price_text.replace("元/斤", "").replace("元/公斤", "").strip())
            results.append({
                "time": time_el.get_text(strip=True),
                "variety": product,
                "origin": place,
                "price": price,
                "change": change_el.get_text(strip=True) if change_el else "-",
            })

        # 全国数据，三条全收录
        assert len(results) == 3
        assert results[0]["variety"] == "白蒜"
        assert results[0]["origin"] == "郑州市中牟县"
        assert results[0]["price"] == 3.85
        assert results[2]["origin"] == "山东潍坊市"

    def test_empty_page_handled(self):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup("", "html.parser")
        items = soup.select("div.quotation-content-list li.market-list-item")
        assert len(items) == 0


class TestAPI:
    def test_status(self, client):
        resp = client.get("/api/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "last_update" in data
        assert "total_records" in data
        assert data["total_records"] == 10

    def test_price(self, client):
        resp = client.get("/api/price")
        assert resp.status_code == 200
        data = resp.json()
        assert "current" in data
        assert "today_high" in data
        assert "today_low" in data

    def test_price_filter(self, client):
        resp = client.get("/api/price?variety=白蒜")
        assert resp.status_code == 200
        assert resp.json()["current"] is not None

    def test_history_hours(self, client):
        resp = client.get("/api/history?hours=24")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0
        assert "time" in data[0]
        assert "price" in data[0]

    def test_history_days(self, client):
        resp = client.get("/api/history?days=7")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_homepage(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "全国大蒜" in resp.text
        assert "chart.js" in resp.text
