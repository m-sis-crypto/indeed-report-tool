# -*- coding: utf-8 -*-
"""
データ倉庫のキャッチコピー・写真説明を一括取得するローカルバッチスクリプト

【使い方】
  py -X utf8 fetch_job_details.py

【初回セットアップ（1回だけ）】
  pip install playwright
  playwright install chromium

【動作】
  データ倉庫スプレッドシートの「求人URLあり・キャッチコピー空」の行を検索し、
  Playwright（ローカルChrome）でページを取得 → Geminiで写真説明を生成 → 倉庫に書き戻す

【.env ファイル】
  このファイルと同じフォルダに .env を作り、以下を記載：
  GEMINI_API_KEY=AIza...
"""

import os
import time
import base64
import re
from collections import defaultdict
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

load_dotenv(Path(__file__).parent / ".env")

# ============================================================
# 設定
# ============================================================
WAREHOUSE_SPREADSHEET_ID = "1Vr7-IpCgEG4Gl2kRhb86Gxz4vYg8R9VpkTEpLNzvuF4"
WAREHOUSE_SHEET_NAME = "データ倉庫"
TOKEN_PATH = Path(r"C:\Users\mgm03\OneDrive\デスクトップ\AIエージェント\tabelog_tool\レビュワー取得\token.json")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# 列インデックス（0始まり・ヘッダー行除く）
COL_CLIENT      = 1    # B列: クライアント
COL_STORE       = 2    # C列: 店舗（正規化名）
COL_STATION     = 6    # G列: 最寄り駅
COL_URL         = 11   # L列: 求人URL
COL_TITLE       = 10   # K列: 求人タイトル
COL_CATCHPHRASE = 19   # T列: キャッチコピー
COL_PHOTO_DESC  = 20   # U列: 写真説明
COL_SALARY      = 21   # V列: 給与

# 写真候補セレクタ（優先度順）
PHOTO_SELECTORS = [
    "[data-testid='ipl-carousel-items-container'] img",  # カルーセル（モバイル版）
    "[class*='jobPhoto'] img",
    "[class*='companyPhoto'] img",
    "[class*='heroImage'] img",
    "[class*='headerImage'] img",
    "[class*='jobImage'] img",
    ".jobsearch-JobComponent-embeddedHeader img",
]


# ============================================================
# Google Sheets
# ============================================================
def get_service():
    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("sheets", "v4", credentials=creds)


def read_warehouse(service):
    result = service.spreadsheets().values().get(
        spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
        range=f"'{WAREHOUSE_SHEET_NAME}'!A:U",
    ).execute()
    return result.get("values", [])


def update_cells(service, sheet_row: int, catchphrase: str, photo_desc: str, salary: str = "", station: str = ""):
    """G列（最寄り駅）・T列（キャッチコピー）・U列（写真説明）・V列（給与）を更新する。sheet_rowは1始まり。"""
    data = [
        {"range": f"'{WAREHOUSE_SHEET_NAME}'!T{sheet_row}", "values": [[catchphrase]]},
        {"range": f"'{WAREHOUSE_SHEET_NAME}'!U{sheet_row}", "values": [[photo_desc]]},
        {"range": f"'{WAREHOUSE_SHEET_NAME}'!V{sheet_row}", "values": [[salary]]},
    ]
    if station:
        data.append({"range": f"'{WAREHOUSE_SHEET_NAME}'!G{sheet_row}", "values": [[station]]})
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


def update_master_stations(service, master_updates: dict):
    """マスターシートの最寄り駅列を一括更新する。
    master_updates: {client_name: {store_name: station}}
    """
    for client_name, store_map in master_updates.items():
        sheet_name = f"マスター_{client_name}"
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
                range=f"'{sheet_name}'!A:E",
            ).execute()
            rows = result.get("values", [])
            if len(rows) < 2:
                continue
            # ヘッダー行でE列（最寄り駅）のインデックスを確認
            header = rows[0]
            station_col_idx = next(
                (i for i, h in enumerate(header) if "最寄り駅" in h), len(header)
            )
            updates = []
            for row_idx, row in enumerate(rows[1:], start=2):  # 2始まり（ヘッダー除く）
                store_name = row[1] if len(row) > 1 else ""  # B列=正規化名
                if store_name in store_map:
                    current_station = row[station_col_idx] if len(row) > station_col_idx else ""
                    if not current_station:
                        col_letter = chr(ord('A') + station_col_idx)
                        updates.append({
                            "range": f"'{sheet_name}'!{col_letter}{row_idx}",
                            "values": [[store_map[store_name]]],
                        })
            if updates:
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
                    body={"valueInputOption": "RAW", "data": updates},
                ).execute()
                print(f"  📋 マスター「{sheet_name}」: {len(updates)}件の最寄り駅を更新しました")
        except Exception as e:
            print(f"  ⚠️  マスター「{sheet_name}」更新エラー: {e}")


# ============================================================
# Playwright スクレイピング
# ============================================================
def scrape_with_playwright(url: str) -> tuple:
    """Indeed求人ページからキャッチコピーと写真URLを取得する。
    ローカルPCのChromeを使うため bot 検知を回避しやすい。
    Returns: (catchphrase: str, photo_url: str)
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  ⚠️  Playwright未インストール。pip install playwright && playwright install chromium を実行してください")
        return "", ""

    catchphrase = ""
    photo_url = ""
    salary = ""
    station = ""

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,  # 画面表示あり（bot検知回避に有効）
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
                ),
                locale="ja-JP",
                viewport={"width": 390, "height": 844},
            )
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=20000)
            page.wait_for_timeout(2000)  # JS描画待ち

            # セキュリティチェックページの検知
            if "Security Check" in page.title() or "security" in page.url.lower():
                print("  ⚠️  Indeedのセキュリティチェックが表示されました。手動で突破してください。")
                page.wait_for_timeout(15000)  # 15秒待機（手動操作の時間）

            # キャッチコピーを探す（JapanJobSubtitle-text が本物のキャッチコピー）
            subtitle_el = page.query_selector("[class*='JapanJobSubtitle-text']")
            if subtitle_el:
                text = (subtitle_el.inner_text() or "").strip()
                if len(text) >= 10:
                    catchphrase = text[:500]

            # 給与を探す（e1wnkr790クラスの中で「円」を含む要素）
            for el in page.query_selector_all("[class*='e1wnkr790']"):
                text = (el.inner_text() or "").strip()
                if "円" in text and len(text) < 60:
                    salary = text
                    break

            # 最寄り駅を探す：「アクセス」セクションの最初のliから抽出
            for section in page.query_selector_all("[class*='JobDescriptionBlockSection']"):
                header = section.query_selector("[class*='JobDescriptionBlockSection-headerText']")
                if header and (header.inner_text() or "").strip() == "アクセス":
                    first_li = section.query_selector("li")
                    if first_li:
                        li_text = (first_li.inner_text() or "").strip()
                        m = re.search(r'「(\S+駅)」', li_text)
                        if m:
                            station = m.group(1)
                    break

            # 写真URLを探す
            for sel in PHOTO_SELECTORS:
                img = page.query_selector(sel)
                if img:
                    src = img.get_attribute("src") or img.get_attribute("data-src") or ""
                    if src and not src.endswith(".gif"):
                        photo_url = src
                        break

            browser.close()

    except Exception as e:
        print(f"  ⚠️  Playwright エラー: {e}")

    return catchphrase, photo_url, salary, station


# ============================================================
# Gemini 写真説明
# ============================================================
def describe_photo(photo_url: str) -> str:
    """写真URLをGeminiに渡して15文字以内の説明文を生成する。"""
    if not photo_url or not GEMINI_API_KEY:
        return ""
    try:
        from google import genai

        img_resp = requests.get(photo_url, timeout=8)
        img_resp.raise_for_status()
        img_data = base64.b64encode(img_resp.content).decode()

        ct = img_resp.headers.get("Content-Type", "image/jpeg")
        mime_type = "image/png" if "png" in ct else "image/webp" if "webp" in ct else "image/jpeg"

        client = genai.Client(api_key=GEMINI_API_KEY)
        resp = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=[{
                "parts": [
                    {
                        "text": (
                            "この求人写真を15文字以内で一言説明してください。"
                            "例：「内装写真（カウンター席）」「外観写真（夜景）」"
                            "「スタッフ写真（女性2名）」「料理写真（寿司盛り合わせ）」"
                            "のような形式で。説明文のみ返してください。"
                        )
                    },
                    {"inline_data": {"mime_type": mime_type, "data": img_data}},
                ]
            }]
        )
        return resp.text.strip()
    except Exception as e:
        print(f"  ⚠️  Gemini エラー: {e}")
        return ""


# ============================================================
# メイン処理
# ============================================================
def main():
    print("=" * 60)
    print("データ倉庫 キャッチコピー・写真一括取得スクリプト")
    print("=" * 60)

    if not GEMINI_API_KEY:
        print("⚠️  .env に GEMINI_API_KEY が設定されていません（写真説明はスキップされます）")

    print("\n📊 データ倉庫に接続中...")
    service = get_service()
    all_values = read_warehouse(service)

    if len(all_values) < 2:
        print("データがありません")
        return

    header = all_values[0]
    data_rows = all_values[1:]
    print(f"  総行数: {len(data_rows)}件")

    # 求人URLがあってキャッチコピーまたは最寄り駅が空の行を抽出
    targets = []
    for i, row in enumerate(data_rows):
        url = row[COL_URL] if len(row) > COL_URL else ""
        catchphrase = row[COL_CATCHPHRASE] if len(row) > COL_CATCHPHRASE else ""
        station = row[COL_STATION] if len(row) > COL_STATION else ""
        # URLがhttpで始まる行のみ対象（古い形式の表示回数等を除外）
        if url and url.startswith("http") and (not catchphrase or not station):
            targets.append((i + 2, row))  # sheet_row（1始まり + ヘッダー行）

    print(f"  取得対象（URLあり・キャッチコピーor最寄り駅空）: {len(targets)}件\n")

    if not targets:
        print("✅ 取得が必要な行はありません")
        return

    success_count = 0
    master_updates = defaultdict(dict)  # {client_name: {store_name: station}}

    for idx, (sheet_row, row) in enumerate(targets):
        url    = row[COL_URL]
        title  = row[COL_TITLE] if len(row) > COL_TITLE else ""
        client = row[COL_CLIENT] if len(row) > COL_CLIENT else ""
        store  = row[COL_STORE] if len(row) > COL_STORE else ""
        existing_station = row[COL_STATION] if len(row) > COL_STATION else ""
        print(f"[{idx+1}/{len(targets)}] {title[:40]}")
        print(f"  {url[:70]}")

        catchphrase, photo_url, salary, station = scrape_with_playwright(url)
        # すでに最寄り駅が入っている行は上書きしない
        if existing_station:
            station = ""
        photo_desc = describe_photo(photo_url) if photo_url else ""

        print(f"  キャッチコピー: {catchphrase[:60] if catchphrase else '（取得できず）'}")
        print(f"  写真説明: {photo_desc if photo_desc else '（取得できず）'}")
        print(f"  給与: {salary if salary else '（取得できず）'}")
        print(f"  最寄り駅: {station if station else '（取得できず）'}")

        if catchphrase or photo_desc or salary or station:
            update_cells(service, sheet_row, catchphrase, photo_desc, salary, station)
            success_count += 1
            if station and client and store:
                master_updates[client][store] = station
            print("  → 倉庫に書き込みました ✅")
        else:
            print("  → スキップ（取得できなかったため書き込まず）")

        # Indeed への連続アクセスを防ぐ
        if idx < len(targets) - 1:
            time.sleep(3)

    # マスターシートにも最寄り駅を反映
    if master_updates:
        print(f"\n📋 マスターシートに最寄り駅を反映中...")
        update_master_stations(service, master_updates)

    print(f"\n{'=' * 60}")
    print(f"✅ 完了: {success_count}/{len(targets)} 件を更新しました")


if __name__ == "__main__":
    main()
