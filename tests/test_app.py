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

    def test_extract_province_by_prefix(self):
        """省份前缀直接匹配"""
        assert app_module.extract_province("山东济南市商河县") == "山东"
        assert app_module.extract_province("河南省郑州市中牟县") == "河南"
        assert app_module.extract_province("江苏徐州市邳州市") == "江苏"
        assert app_module.extract_province("北京新发地农副产品批发市场") == "北京"
        assert app_module.extract_province("上海市江桥批发市场") == "上海"

    def test_extract_province_by_city(self):
        """通过城市名反查省份"""
        assert app_module.extract_province("洛阳宏进农副产品批发市场") == "河南"
        assert app_module.extract_province("寿光地利农产品物流园") == "山东"
        assert app_module.extract_province("成都农产品中心批发市场") == "四川"
        assert app_module.extract_province("武汉白沙洲农副产品大市场") == "湖北"
        assert app_module.extract_province("内蒙赤峰西城市场") == "内蒙古"
        assert app_module.extract_province("中牟") == "河南"
        assert app_module.extract_province("杞县") == "河南"

    def test_extract_province_unknown(self):
        """无法识别的返回其他"""
        assert app_module.extract_province("某某未知市场") == "其他"

    def test_parse_stockstar_data_line(self):
        """测试证监会之星报告数据行解析"""
        import re
        data_line = "市场 价格   北京京丰岳各庄农副产品批发市场 9.00   天津何庄子农产品批发市场 8.00   山东金乡大蒜专业批发市场 3.50   单位：元/公斤数据来源：农业农村部信息中心"
        cleaned = data_line.replace("市场 价格   ", "").replace("市场 价格    ", "")
        cleaned = re.sub(r"\s*单位：元/公斤.*$", "", cleaned)
        matches = re.findall(r"([一-鿿（）()·　Ⅰ-Ⅻ]+?)\s+(\d+\.?\d*)", cleaned)
        assert len(matches) == 3
        assert matches[0][0] == "北京京丰岳各庄农副产品批发市场"
        assert matches[0][1] == "9.00"
        assert matches[2][0] == "山东金乡大蒜专业批发市场"
        assert matches[2][1] == "3.50"


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

    def test_provinces(self, client):
        resp = client.get("/api/provinces")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, dict)
        assert "data" in body
        assert "provinces" in body
        assert "origins" in body
        data = body["data"]
        assert isinstance(data, list)
        assert len(data) > 0
        for p in data:
            assert "province" in p
            assert "time" in p
            assert "variety" in p
            assert "origin" in p
            assert "price" in p
        prices = [p["price"] for p in data]
        assert prices == sorted(prices, reverse=True)
        # 测省份筛选
        resp2 = client.get("/api/provinces?province=湖北")
        assert resp2.status_code == 200
        body2 = resp2.json()
        for p in body2["data"]:
            assert p["province"] == "湖北"
