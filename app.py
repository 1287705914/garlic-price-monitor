"""河南大蒜价格实时监控系统"""
import sqlite3
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "garlic.db")

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

app = FastAPI(title="河南大蒜价格监控")

@app.get("/api/status")
def api_status():
    return {"status": "ok"}
