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
    def test_parse_html(self):
        sample_html = """
        <html><body>
        <table class="price-table">
            <tr><th>品种</th><th>产地</th><th>价格</th></tr>
            <tr><td>白蒜</td><td>中牟</td><td>3.85</td></tr>
            <tr><td>红蒜</td><td>杞县</td><td>3.60</td></tr>
            <tr><td>杂交蒜</td><td>中牟</td><td>3.42</td></tr>
        </table>
        </body></html>"""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(sample_html, "html.parser")
        rows = soup.select("table.price-table tr")
        results = []
        for row in rows[1:]:
            cols = row.find_all("td")
            results.append({
                "variety": cols[0].text.strip(),
                "origin": cols[1].text.strip(),
                "price": float(cols[2].text.strip()),
            })
        assert len(results) == 3
        assert results[0] == {"variety": "白蒜", "origin": "中牟", "price": 3.85}

    def test_empty_response_handled(self):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup("", "html.parser")
        rows = soup.select("table.price-table tr")
        results = []
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) >= 3:
                results.append({})
        assert results == []


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
        assert "河南大蒜" in resp.text
        assert "chart.js" in resp.text
