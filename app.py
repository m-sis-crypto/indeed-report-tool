# -*- coding: utf-8 -*-
"""
Indeed レポート Streamlit UI
起動: streamlit run app.py  （または 起動.bat をダブルクリック）
"""

import csv
import io
import json
import urllib.parse
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from indeed_report import (
    aggregate,
    aggregate_detail,
    build_rows,
    build_rows_detail,
    build_rows_unknown,
    load_job_role_rules,
    parse_period_from_filename,
)

# ============================================================
# パス設定
# ============================================================
BASE_DIR     = Path(__file__).parent
CONFIG_DIR   = BASE_DIR / "config"
CLIENTS_PATH = CONFIG_DIR / "clients.json"
RULES_PATH   = CONFIG_DIR / "job_role_rules.csv"
MASTERS_DIR  = BASE_DIR / "masters"

CONFIG_DIR.mkdir(exist_ok=True)
MASTERS_DIR.mkdir(exist_ok=True)

TOKEN_PATH = Path(r"C:\Users\mgm03\OneDrive\デスクトップ\AIエージェント\tabelog_tool\レビュワー取得\token.json")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
COLS = ["店舗", "職種", "雇用形態", "掲載開始", "掲載終了", "表示回数", "クリック数", "応募開始数", "応募数", "費用", "参照番号"]

# ============================================================
# データ倉庫設定
# ============================================================
WAREHOUSE_SPREADSHEET_ID = "1Vr7-IpCgEG4Gl2kRhb86Gxz4vYg8R9VpkTEpLNzvuF4"
WAREHOUSE_SHEET_NAME     = "データ倉庫"
CONFIG_SHEET_NAME        = "クライアント設定"
WAREHOUSE_COLS = [
    "取込日", "クライアント", "店舗", "大カテゴリ", "業態", "エリア", "最寄り駅",
    "職種", "雇用形態", "参照番号", "求人タイトル", "求人URL",
    "集計開始", "集計終了",
    "表示回数", "クリック数", "応募開始数", "応募数", "費用",
    "キャッチコピー", "写真説明", "給与",
]

# 業態の選択肢（大カテゴリ別）
GENRE_OPTIONS = [
    "",
    # 飲食
    "イタリアン", "ピッツェリア", "フレンチ", "スペイン料理", "洋食・西洋料理",
    "和食", "寿司", "そば・うどん", "中華料理", "ラーメン", "アジア・エスニック",
    "居酒屋・ダイニングバー", "バー", "カフェ・喫茶店", "パン屋・ベーカリー",
    "ケーキ屋・スイーツ", "ファーストフード・ファミレス", "テイクアウト・惣菜・弁当屋",
    "焼肉", "焼き鳥", "専門料理　他", "ホテル・旅館",
    "給食・社員食堂・病院・介護", "食品工場・セントラルキッチン",
    # 美容（先々追加予定）
    # 小売（先々追加予定）
]

_MASTER_COLS_JP = ["Indeed企業名", "正規化名", "大カテゴリ", "業態", "エリア", "最寄り駅", "キーワード（カンマ区切り）"]
_MASTER_COLS_EN = ["store_name", "short_name", "category", "genre", "area", "nearest_station", "keywords"]
_MASTER_JP2EN   = dict(zip(_MASTER_COLS_JP, _MASTER_COLS_EN))
_MASTER_EN2JP   = dict(zip(_MASTER_COLS_EN, _MASTER_COLS_JP))

# ============================================================
# デフォルト設定
# ============================================================
_DEFAULT_CLIENTS = {
    "ALLSTARTED": {
        "master_path":    "masters/allstarted.csv",
        "spreadsheet_id": "1bxHZxhPFzgoz-xXSk8b7Onk5G-N6KcwehAZMiqiJqM4",
        "sheet_pattern1": "レポート抽出",
        "sheet_pattern2": "レポート抽出_詳細",
    },
    "TOU": {
        "master_path":    "masters/tou.csv",
        "spreadsheet_id": "1zfQZ_Puyus9nJLGE3iNpSoJmlqUMdhS8uCS79EHCJe4",
        "sheet_pattern1": "レポート抽出",
        "sheet_pattern2": "レポート抽出_詳細",
    },
}

_DEFAULT_RULES_DF = pd.DataFrame([
    {"正規化後の職種名": "調理補助",     "キーワード（カンマ区切り）": "調理補助,キッチン補助"},
    {"正規化後の職種名": "調理",         "キーワード（カンマ区切り）": "調理,キッチン"},
    {"正規化後の職種名": "ホール",       "キーワード（カンマ区切り）": "ホール"},
    {"正規化後の職種名": "店舗",         "キーワード（カンマ区切り）": "店舗"},
    {"正規化後の職種名": "深夜アルバイト", "キーワード（カンマ区切り）": "深夜アルバイト"},
    {"正規化後の職種名": "アルバイト",   "キーワード（カンマ区切り）": "アルバイト"},
])


# ============================================================
# Sheets API サービス
# ============================================================
def get_service():
    if "gcp_refresh_token" in st.secrets:
        creds = Credentials.from_authorized_user_info({
            "refresh_token": st.secrets["gcp_refresh_token"],
            "token_uri":     st.secrets["gcp_token_uri"],
            "client_id":     st.secrets["gcp_client_id"],
            "client_secret": st.secrets["gcp_client_secret"],
        }, SCOPES)
    else:
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("sheets", "v4", credentials=creds)


@st.cache_resource(show_spinner=False)
def _build_service_cached():
    try:
        return get_service()
    except Exception:
        return None


def get_or_init_service():
    return _build_service_cached()


# ============================================================
# クラウド設定ストア（Google Sheetsに永続化）
# ============================================================
def _ensure_sheet(service, spreadsheet_id: str, sheet_name: str):
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    if not any(s["properties"]["title"] == sheet_name for s in spreadsheet["sheets"]):
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
        ).execute()


def _master_sheet_name(client_name: str) -> str:
    return f"マスター_{client_name}"


def load_clients_from_sheets(service) -> dict:
    """WarehouseスプレッドシートのCONFIG_SHEET_NAMEシートからクライアント設定を読む"""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
            range=f"'{CONFIG_SHEET_NAME}'!A:D"
        ).execute()
        values = result.get("values", [])
        if len(values) < 2:
            return {}
        clients = {}
        for row in values[1:]:
            name = row[0].strip() if row else ""
            if not name:
                continue
            clients[name] = {
                "master_path":    f"masters/{name.lower()}.csv",
                "spreadsheet_id": row[1].strip() if len(row) > 1 else "",
                "sheet_pattern1": row[2].strip() if len(row) > 2 else "",
                "sheet_pattern2": row[3].strip() if len(row) > 3 else "",
            }
        return clients
    except Exception:
        return {}


def save_clients_to_sheets(service, clients: dict):
    """クライアント設定をWarehouseスプレッドシートに保存"""
    _ensure_sheet(service, WAREHOUSE_SPREADSHEET_ID, CONFIG_SHEET_NAME)
    rows = [["クライアント名", "スプレッドシートID", "シート名①", "シート名②"]]
    for name, cfg in clients.items():
        rows.append([
            name,
            cfg.get("spreadsheet_id", ""),
            cfg.get("sheet_pattern1", ""),
            cfg.get("sheet_pattern2", ""),
        ])
    service.spreadsheets().values().clear(
        spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
        range=f"'{CONFIG_SHEET_NAME}'!A:D"
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
        range=f"'{CONFIG_SHEET_NAME}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()


def load_master_from_sheets(service, client_name: str) -> pd.DataFrame:
    """Warehouseスプレッドシートからクライアントの店舗マスターを読む"""
    sheet_name = _master_sheet_name(client_name)
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
            range=f"'{sheet_name}'!A:G"
        ).execute()
        values = result.get("values", [])
        if len(values) < 2:
            return pd.DataFrame(columns=_MASTER_COLS_JP)
        header = values[0]
        data = []
        for row in values[1:]:
            padded = row + [""] * (len(header) - len(row))
            data.append(dict(zip(header, padded)))
        df = pd.DataFrame(data).rename(columns=_MASTER_EN2JP)
        for col in _MASTER_COLS_JP:
            if col not in df.columns:
                df[col] = ""
        return df[_MASTER_COLS_JP]
    except Exception:
        return pd.DataFrame(columns=_MASTER_COLS_JP)


def save_master_to_sheets(service, client_name: str, df: pd.DataFrame):
    """クライアントの店舗マスターをWarehouseスプレッドシートに保存"""
    sheet_name = _master_sheet_name(client_name)
    _ensure_sheet(service, WAREHOUSE_SPREADSHEET_ID, sheet_name)
    out = df.rename(columns=_MASTER_JP2EN)
    rows = [_MASTER_COLS_EN]
    for _, row in out.iterrows():
        rows.append([str(row.get(col, "")) for col in _MASTER_COLS_EN])
    service.spreadsheets().values().clear(
        spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
        range=f"'{sheet_name}'!A:G"
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
        range=f"'{sheet_name}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()


# ============================================================
# 設定ファイルの読み書き（Sheets優先・ローカルフォールバック）
# ============================================================
def load_clients() -> dict:
    """クライアント設定を読む。Sheets → ローカルJSON → デフォルトの順。"""
    if "_clients_cache" in st.session_state:
        return st.session_state["_clients_cache"]

    service = get_or_init_service()
    if service:
        sheets_data = load_clients_from_sheets(service)
        if sheets_data:
            st.session_state["_clients_cache"] = sheets_data
            return sheets_data

    # ローカルファイルにフォールバック
    local_data = _DEFAULT_CLIENTS
    if CLIENTS_PATH.exists():
        try:
            local_data = json.loads(CLIENTS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass

    st.session_state["_clients_cache"] = local_data
    return local_data


def save_clients(clients: dict):
    """クライアント設定を保存（Sheets + ローカル）"""
    # ローカル保存
    try:
        CLIENTS_PATH.write_text(json.dumps(clients, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    # セッションキャッシュ更新
    st.session_state["_clients_cache"] = clients
    # Sheets保存
    service = get_or_init_service()
    if service:
        try:
            save_clients_to_sheets(service, clients)
        except Exception as e:
            st.warning(f"クラウド同期に失敗しました（ローカル保存は完了）: {e}")


def clients_to_df(clients: dict) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "クライアント名":    name,
            "スプレッドシートID": cfg["spreadsheet_id"],
            "シート名①":         cfg["sheet_pattern1"],
            "シート名②":         cfg["sheet_pattern2"],
        }
        for name, cfg in clients.items()
    ])


def df_to_clients(df: pd.DataFrame) -> dict:
    clients = {}
    for _, row in df.iterrows():
        name = str(row["クライアント名"]).strip()
        if not name:
            continue
        raw2 = str(row["シート名②"]).strip()
        clients[name] = {
            "master_path":    f"masters/{name.lower()}.csv",
            "spreadsheet_id": str(row["スプレッドシートID"]).strip(),
            "sheet_pattern1": str(row["シート名①"]).strip(),
            "sheet_pattern2": "" if raw2 in ("nan", "NaN", "None", "-") else raw2,
        }
    return clients


def load_rules_df() -> pd.DataFrame:
    if RULES_PATH.exists():
        df = pd.read_csv(RULES_PATH, encoding="utf-8-sig")
        df.columns = ["正規化後の職種名", "キーワード（カンマ区切り）"]
        return df
    return _DEFAULT_RULES_DF.copy()


def save_rules_df(df: pd.DataFrame):
    out = df.rename(columns={"正規化後の職種名": "canonical", "キーワード（カンマ区切り）": "keywords"})
    out.to_csv(RULES_PATH, index=False, encoding="utf-8-sig")


def _write_master_local(df: pd.DataFrame, master_path: str):
    """indeed_report.py互換のためローカルCSVに書き出す"""
    p = BASE_DIR / master_path
    p.parent.mkdir(exist_ok=True)
    out = df.rename(columns=_MASTER_JP2EN)
    cols = [c for c in _MASTER_COLS_EN if c in out.columns]
    out[cols].to_csv(p, index=False, encoding="utf-8-sig")


def load_master_df(master_path: str, client_name: str = None) -> pd.DataFrame:
    """店舗マスターを読む。セッションキャッシュ → Sheets → ローカルCSV の順。"""
    cache_key = f"_master_cache_{client_name}"
    if cache_key in st.session_state:
        return st.session_state[cache_key]

    empty = pd.DataFrame(columns=_MASTER_COLS_JP)

    service = get_or_init_service()
    if client_name and service:
        sheets_df = load_master_from_sheets(service, client_name)
        if not sheets_df.empty:
            try:
                _write_master_local(sheets_df, master_path)
            except Exception:
                pass
            st.session_state[cache_key] = sheets_df
            return sheets_df

    # ローカルCSVにフォールバック
    p = BASE_DIR / master_path
    if not p.exists():
        st.session_state[cache_key] = empty
        return empty
    with open(p, encoding="utf-8-sig") as f:
        rows_raw = list(csv.DictReader(f))
    if not rows_raw:
        st.session_state[cache_key] = empty
        return empty
    df = pd.DataFrame([
        {
            "Indeed企業名":              r["store_name"],
            "正規化名":                  r["short_name"],
            "大カテゴリ":                r.get("category", ""),
            "業態":                      r.get("genre", ""),
            "エリア":                    r.get("area", ""),
            "最寄り駅":                  r.get("nearest_station", ""),
            "キーワード（カンマ区切り）": r["keywords"],
        }
        for r in rows_raw
    ])
    st.session_state[cache_key] = df
    return df


def save_master_df(df: pd.DataFrame, master_path: str, client_name: str = None):
    """店舗マスターを保存（Sheets + ローカル）"""
    _write_master_local(df, master_path)
    if client_name:
        # セッションキャッシュを更新（次回renderでAPIを叩かないようにする）
        st.session_state[f"_master_cache_{client_name}"] = df
        service = get_or_init_service()
        if service:
            try:
                save_master_to_sheets(service, client_name, df)
            except Exception as e:
                st.warning(f"クラウド同期に失敗しました（ローカル保存は完了）: {e}")


def master_df_to_list(df: pd.DataFrame) -> list:
    """DataFrameをaggregate()等が期待するmaster listに変換"""
    result = []
    for _, row in df.iterrows():
        keywords = [k.strip() for k in str(row.get("キーワード（カンマ区切り）", "")).split(",") if k.strip()]
        result.append({
            "store":           str(row.get("Indeed企業名", "")),
            "short_name":      str(row.get("正規化名", "")),
            "keywords":        keywords,
            "category":        str(row.get("大カテゴリ", "")),
            "genre":           str(row.get("業態", "")),
            "area":            str(row.get("エリア", "")),
            "nearest_station": str(row.get("最寄り駅", "")),
        })
    return result


def get_rules():
    return load_job_role_rules(str(RULES_PATH)) if RULES_PATH.exists() else None


# ============================================================
# AI 正規化名推測
# ============================================================
def suggest_store_labels(company_names: list) -> dict:
    """Geminiで企業名→正規化名・大カテゴリ・業態を一括推測。"""
    from google import genai

    api_key = st.secrets.get("gemini_api_key", "")
    if not api_key:
        raise ValueError("Streamlit SecretsにGemini APIキー（gemini_api_key）が設定されていません")

    client = genai.Client(api_key=api_key)
    category_options = ["飲食", "美容", "小売", "医療・介護", "その他"]
    genre_options = [g for g in GENRE_OPTIONS if g]

    names_text = "\n".join(f"- {n}" for n in company_names)
    prompt = f"""以下はIndeedの求人に掲載された企業名（店舗名）の一覧です。
それぞれについて、正規化名・大カテゴリ・業態を推測してください。

【正規化名のルール】
- 業態説明（例：「中華ビストロ」「焼鳥ビストロ」「New」）は除く
- 英語サブタイトル（例：「- Nonotori」）は除く
- 支店・エリア名（例：人形町・新宿・赤坂）はそのまま残す
- シンプルで短い名前にする

【大カテゴリの選択肢（必ずこの中から選ぶ）】
{", ".join(category_options)}

【業態の選択肢（必ずこの中から選ぶ）】
{", ".join(genre_options)}

【企業名一覧】
{names_text}

【出力形式】必ずJSONのみを返してください（説明文不要）
[
  {{
    "original": "元の企業名",
    "normalized": "推測した正規化名",
    "category": "大カテゴリ",
    "genre": "業態"
  }},
  ...
]"""

    response = client.models.generate_content(model="gemini-2.5-flash-lite", contents=prompt)
    text = response.text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    parsed = json.loads(text.strip())
    return {
        item["original"]: {
            "normalized": item.get("normalized", ""),
            "category":   item.get("category", ""),
            "genre":      item.get("genre", ""),
        }
        for item in parsed
    }


# ============================================================
# Indeed 求人ページ スクレイピング
# ============================================================
def scrape_job_details(url: str, timeout: int = 8) -> tuple:
    """Indeed求人ページからキャッチコピーと写真URLを取得する。
    Returns: (catchphrase: str, photo_url: str)  取得失敗時は ("", "")
    """
    if not url:
        return "", ""
    try:
        import requests
        from bs4 import BeautifulSoup

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
            ),
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        soup = BeautifulSoup(resp.text, "html.parser")

        # キャッチコピーを探す（優先度順）
        catchphrase = ""
        for sel in [
            # PRテキスト専用ブロック（ブックマークアイコン付きカード）
            "[data-testid='pr-text']",
            "[class*='prText']",
            "[class*='pr-text']",
            "[class*='prDescription']",
            "[class*='pr_description']",
            "[class*='PrDescription']",
            # 求人説明の先頭段落（代替）
            "[data-testid='jobDescriptionText'] > p:first-of-type",
            "#jobDescriptionText > p:first-of-type",
        ]:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(separator="", strip=True)
                if len(text) >= 20:
                    catchphrase = text[:500]  # 上限500文字
                    break

        # 写真URLを探す（求人ページのメイン画像）
        photo_url = ""
        for sel in [
            "[class*='jobPhoto'] img",
            "[class*='companyPhoto'] img",
            "[class*='heroImage'] img",
            "[class*='headerImage'] img",
            "[class*='jobImage'] img",
            "[data-testid*='photo'] img",
            ".jobsearch-JobComponent-embeddedHeader img",
        ]:
            img = soup.select_one(sel)
            if img:
                src = img.get("src", "") or img.get("data-src", "")
                if src and not src.endswith(".gif"):
                    photo_url = src
                    break

        return catchphrase, photo_url

    except Exception:
        return "", ""


def describe_photo(photo_url: str) -> str:
    """写真URLをGeminiに渡して10文字程度の説明文を生成する。
    Returns: 説明文（例：「内装写真（カウンター席）」）または ""
    """
    if not photo_url:
        return ""
    try:
        import base64
        import requests as _req
        from google import genai

        api_key = st.secrets.get("gemini_api_key", "")
        if not api_key:
            return ""

        img_resp = _req.get(photo_url, timeout=6)
        img_resp.raise_for_status()
        img_data = base64.b64encode(img_resp.content).decode()

        content_type = img_resp.headers.get("Content-Type", "image/jpeg")
        if "png" in content_type:
            mime_type = "image/png"
        elif "webp" in content_type:
            mime_type = "image/webp"
        else:
            mime_type = "image/jpeg"

        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
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
        return response.text.strip()

    except Exception:
        return ""


def batch_scrape(rows_raw: list) -> dict:
    """URLリストを順番にスクレイピングしてキャッシュを返す。
    Returns: {url: (catchphrase, photo_desc)}
    """
    unique_urls = list(dict.fromkeys(
        r.get("求人URL", "") for r in rows_raw if r.get("求人URL", "")
    ))
    if not unique_urls:
        return {}

    result = {}
    pb = st.progress(0.0, text="求人ページを取得中...")
    for i, url in enumerate(unique_urls):
        catchphrase, photo_url = scrape_job_details(url)
        photo_desc = describe_photo(photo_url) if photo_url else ""
        result[url] = (catchphrase, photo_desc)
        pb.progress((i + 1) / len(unique_urls), text=f"取得中… {i+1}/{len(unique_urls)}")
    pb.empty()
    return result


# ============================================================
# Sheets ヘルパー（レポート書き込み用）
# ============================================================
def get_last_row(service, spreadsheet_id, sheet_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A:A"
    ).execute()
    return len(result.get("values", []))


def get_sheet_id_num(service, spreadsheet_id, sheet_name):
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in spreadsheet["sheets"]:
        if sheet["properties"]["title"] == sheet_name:
            return sheet["properties"]["sheetId"]
    raise ValueError(f"シートが見つかりません: {sheet_name}")


def delete_period_rows(service, spreadsheet_id, sheet_name, period_start, period_end):
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!D:E"
    ).execute()
    values = result.get("values", [])
    rows_to_delete = [
        i for i, row in enumerate(values)
        if len(row) >= 2 and row[0] == period_start and row[1] == period_end
    ]
    if not rows_to_delete:
        return 0
    sheet_id_num = get_sheet_id_num(service, spreadsheet_id, sheet_name)
    requests = [
        {"deleteDimension": {"range": {
            "sheetId": sheet_id_num, "dimension": "ROWS",
            "startIndex": i, "endIndex": i + 1,
        }}}
        for i in sorted(rows_to_delete, reverse=True)
    ]
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()
    return len(rows_to_delete)


def append_to_sheet(service, spreadsheet_id, sheet_name, rows, next_row):
    range_str = f"'{sheet_name}'!A{next_row}:K{next_row + len(rows) - 1}"
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_str,
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()


# ============================================================
# データ倉庫ヘルパー
# ============================================================
def ensure_warehouse_sheet(service):
    spreadsheet = service.spreadsheets().get(spreadsheetId=WAREHOUSE_SPREADSHEET_ID).execute()
    exists = any(s["properties"]["title"] == WAREHOUSE_SHEET_NAME for s in spreadsheet["sheets"])
    if not exists:
        service.spreadsheets().batchUpdate(
            spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": WAREHOUSE_SHEET_NAME}}}]},
        ).execute()
    result = service.spreadsheets().values().get(
        spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
        range=f"'{WAREHOUSE_SHEET_NAME}'!A1:A1"
    ).execute()
    if not result.get("values"):
        service.spreadsheets().values().update(
            spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
            range=f"'{WAREHOUSE_SHEET_NAME}'!A1",
            valueInputOption="RAW",
            body={"values": [WAREHOUSE_COLS]},
        ).execute()


def build_warehouse_rows(client_name, rows_raw, master, rules, period_start, period_end, scraped=None):
    """1求人1行でデータ倉庫用の行リストを生成する。
    scraped: {url: (catchphrase, photo_desc)} — batch_scrape() の結果を渡すとキャッチコピー・写真説明が入る
    """
    from indeed_report import normalize_store, extract_employment_type, extract_job_title, to_int, to_float
    today = date.today().strftime("%Y/%m/%d")
    scraped = scraped or {}
    store_labels = {
        e["short_name"]: (
            e.get("category", ""), e.get("genre", ""),
            e.get("area", ""), e.get("nearest_station", ""),
        )
        for e in master
    }
    out = []
    for row in rows_raw:
        short_name = normalize_store(row["企業名"], master)
        if not short_name:
            continue
        emp_type  = extract_employment_type(row["求人"], row.get("キャンペーン", ""))
        job_title = extract_job_title(row["求人"], rules)
        cat, genre, area, station = store_labels.get(short_name, ("", "", "", ""))
        url = row.get("求人URL", "")
        catchphrase, photo_desc = scraped.get(url, ("", ""))
        out.append([
            today, client_name, short_name, cat, genre, area, station,
            job_title, emp_type,
            row.get("参照番号", ""),
            row.get("求人", ""),
            url,
            period_start, period_end,
            to_int(row["表示回数"]), to_int(row["クリック数"]),
            to_int(row["応募開始数"]), to_int(row["応募数"]),
            round(to_float(row["費用"])),
            catchphrase,
            photo_desc,
        ])
    return out


def append_to_warehouse(service, warehouse_rows):
    ensure_warehouse_sheet(service)
    last = get_last_row(service, WAREHOUSE_SPREADSHEET_ID, WAREHOUSE_SHEET_NAME)
    next_row = last + 1
    range_str = f"'{WAREHOUSE_SHEET_NAME}'!A{next_row}:V{next_row + len(warehouse_rows) - 1}"
    service.spreadsheets().values().update(
        spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
        range=range_str,
        valueInputOption="RAW",
        body={"values": warehouse_rows},
    ).execute()


# ============================================================
# UI
# ============================================================
st.set_page_config(page_title="Indeed レポートツール", layout="wide")
st.title("📊 Indeed レポート自動整理ツール")

main_tab, settings_tab = st.tabs(["📊 レポート", "⚙️ 設定"])

# ============================================================
# ⚙️ 設定タブ
# ============================================================
with settings_tab:
    s1, s2, s3 = st.tabs(["🏢 クライアント管理", "📋 職種ルール", "🗂 店舗マスター"])

    # ─── クライアント管理 ─────────────────────────────────────
    with s1:
        st.subheader("クライアント一覧")
        st.caption("行をクリックして直接編集できます。下の「＋ 行を追加」で新規クライアントを追加してください。")

        clients_cfg = load_clients()
        edited_clients_df = st.data_editor(
            clients_to_df(clients_cfg),
            num_rows="dynamic",
            hide_index=True,
            key="clients_editor",
            column_config={
                "クライアント名":    st.column_config.TextColumn("クライアント名", width="small"),
                "スプレッドシートID": st.column_config.TextColumn("スプレッドシートID", width="large"),
                "シート名①":         st.column_config.TextColumn("シート名①（集計）", width="medium"),
                "シート名②":         st.column_config.TextColumn("シート名②（詳細・省略可）", width="medium"),
            },
        )
        st.caption("💡 設定はGoogleスプレッドシートに保存されるため、アプリを再起動しても消えません。")

        if st.button("💾 クライアントを保存", key="save_clients_btn"):
            new_clients = df_to_clients(edited_clients_df)
            save_clients(new_clients)
            st.success("✅ 保存しました（Google Sheetsに同期済み）")
            st.rerun()

    # ─── 職種ルール ──────────────────────────────────────────
    with s2:
        st.subheader("職種の正規化ルール")
        st.caption("**上から順に評価**されます。先にマッチしたルールが優先されます。")

        edited_rules_df = st.data_editor(
            load_rules_df(),
            num_rows="dynamic",
            hide_index=True,
            column_config={
                "正規化後の職種名":           st.column_config.TextColumn("正規化後の職種名", width="medium"),
                "キーワード（カンマ区切り）": st.column_config.TextColumn("キーワード（カンマ区切り）", width="large"),
            },
        )
        st.caption("例：「調理,キッチン」と書くと、どちらかが含まれていれば「調理」として集計されます。")

        if st.button("💾 職種ルールを保存", key="save_rules"):
            save_rules_df(edited_rules_df)
            st.success("✅ 保存しました")
            st.rerun()

        st.divider()
        st.subheader("CSVからインポート")

        rules_template = io.StringIO()
        pd.DataFrame(columns=["正規化後の職種名", "キーワード（カンマ区切り）"]).to_csv(rules_template, index=False)
        st.download_button(
            "📥 テンプレートCSVをダウンロード",
            rules_template.getvalue().encode("utf-8-sig"),
            file_name="職種ルール_テンプレート.csv",
            mime="text/csv",
            key="dl_rules_template",
        )

        rules_import = st.file_uploader("職種ルールCSVをアップロード", type=["csv"], key="import_rules")
        if rules_import:
            df_ri = pd.read_csv(rules_import, encoding="utf-8-sig")
            col_map = {}
            for c in df_ri.columns:
                if "canonical" in c or "正規化" in c:
                    col_map[c] = "正規化後の職種名"
                elif "keyword" in c.lower() or "キーワード" in c:
                    col_map[c] = "キーワード（カンマ区切り）"
            df_ri = df_ri.rename(columns=col_map)[["正規化後の職種名", "キーワード（カンマ区切り）"]]
            st.dataframe(df_ri, hide_index=True)
            st.caption(f"{len(df_ri)}行を読み込みました")
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("➕ 既存に追記", key="rules_append"):
                    merged = pd.concat([load_rules_df(), df_ri], ignore_index=True).drop_duplicates(subset=["正規化後の職種名"])
                    save_rules_df(merged)
                    st.success(f"✅ 追記しました（計{len(merged)}件）")
                    st.rerun()
            with col_b:
                if st.button("🔄 上書き（全件置き換え）", key="rules_replace"):
                    save_rules_df(df_ri)
                    st.success(f"✅ 上書きしました（{len(df_ri)}件）")
                    st.rerun()

    # ─── 店舗マスター ────────────────────────────────────────
    with s3:
        st.subheader("店舗マスター（表記ゆれ対応）")
        st.caption("IndeedのCSVに出てくる企業名 → レポートに表示する正規化名 のマッピングです。")

        clients_for_master = load_clients()
        if not clients_for_master:
            st.info("まず「🏢 クライアント管理」タブでクライアントを登録してください。")
        else:
            sel_client = st.selectbox("クライアントを選択", list(clients_for_master.keys()), key="master_client_sel")
            m_path = clients_for_master[sel_client]["master_path"]

            _loaded_master_df = load_master_df(m_path, sel_client)
            edited_master_df = st.data_editor(
                _loaded_master_df,
                num_rows="dynamic",
                hide_index=True,
                column_config={
                    "Indeed企業名":          st.column_config.TextColumn("Indeed企業名（CSVの表記通りに入力）", width="large"),
                    "正規化名":              st.column_config.TextColumn("正規化名（レポートに表示する名前）", width="medium"),
                    "大カテゴリ":            st.column_config.SelectboxColumn(
                        "大カテゴリ", width="small",
                        options=["", "飲食", "美容", "小売", "医療・介護", "その他"],
                    ),
                    "業態":                  st.column_config.SelectboxColumn(
                        "業態", width="medium",
                        options=GENRE_OPTIONS,
                    ),
                    "エリア":                st.column_config.TextColumn("エリア（例：新宿・渋谷・銀座）", width="medium"),
                    "最寄り駅":              st.column_config.TextColumn("最寄り駅（例：新宿駅）", width="medium"),
                    "キーワード（カンマ区切り）": st.column_config.TextColumn("マッチキーワード（カンマ区切り）", width="large"),
                },
            )
            st.caption("💡 大カテゴリ・業態・エリア・最寄り駅はデータ倉庫の分析に使われます。設定はGoogle Sheetsに保存されるため再起動後も維持されます。")

            if st.button("💾 マスターを保存", key="save_master"):
                save_master_df(edited_master_df, m_path, sel_client)
                st.success(f"✅ {sel_client} のマスターを保存しました（Google Sheetsに同期済み）")
                st.rerun()

            # 最寄り駅 未入力チェック（同じキャッシュを使う・APIコールなし）
            _missing_station = _loaded_master_df[_loaded_master_df["最寄り駅"].fillna("").str.strip() == ""]
            if not _missing_station.empty:
                with st.expander(f"🗺️ 最寄り駅が未入力の店舗 {len(_missing_station)}件 ── Googleマップで確認して入力してください"):
                    st.caption("最寄り駅はデータ倉庫の分析に必要な情報です。下のリンクでGoogleマップを開き、確認してから上の表に入力・保存してください。")
                    for _, _row in _missing_station.iterrows():
                        _name = str(_row["正規化名"]).strip() or str(_row["Indeed企業名"]).strip()
                        _area = str(_row.get("エリア", "")).strip()
                        _query = urllib.parse.quote(f"{_name} {_area}".strip())
                        _maps_url = f"https://www.google.com/maps/search/{_query}"
                        st.markdown(f"- **{_name}**（{_area}） → [Googleマップで検索]({_maps_url})")

            st.divider()
            st.subheader("CSVからインポート")

            import_mode = st.radio(
                "インポート方法を選択",
                ["マスターCSVをそのままインポート", "IndeedのCSVから企業名を抽出"],
                horizontal=True,
                key="import_mode",
            )

            if import_mode == "マスターCSVをそのままインポート":
                master_template = io.StringIO()
                pd.DataFrame(columns=["store_name", "short_name", "category", "genre", "keywords"]).to_csv(master_template, index=False)
                st.download_button(
                    "📥 テンプレートCSVをダウンロード",
                    master_template.getvalue().encode("utf-8-sig"),
                    file_name="店舗マスター_テンプレート.csv",
                    mime="text/csv",
                    key="dl_master_template",
                )
                st.caption("store_name=Indeed企業名、short_name=正規化名、category=大カテゴリ、genre=業態、keywords=キーワード（カンマ区切り）")

                master_import = st.file_uploader("マスターCSVをアップロード", type=["csv"], key="import_master_csv")
                if master_import:
                    df_mi = pd.read_csv(master_import, encoding="utf-8-sig")
                    col_map = {}
                    for c in df_mi.columns:
                        if c in ("store_name", "Indeed企業名"):
                            col_map[c] = "Indeed企業名"
                        elif c in ("short_name", "正規化名"):
                            col_map[c] = "正規化名"
                        elif c in ("category", "大カテゴリ"):
                            col_map[c] = "大カテゴリ"
                        elif c in ("genre", "業態"):
                            col_map[c] = "業態"
                        elif c in ("keywords", "キーワード（カンマ区切り）"):
                            col_map[c] = "キーワード（カンマ区切り）"
                    df_mi = df_mi.rename(columns=col_map)
                    for col in ["大カテゴリ", "業態"]:
                        if col not in df_mi.columns:
                            df_mi[col] = ""
                    df_mi = df_mi[["Indeed企業名", "正規化名", "大カテゴリ", "業態", "キーワード（カンマ区切り）"]]
                    st.dataframe(df_mi, hide_index=True)
                    st.caption(f"{len(df_mi)}行を読み込みました")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("➕ 既存に追記", key="master_append"):
                            existing = load_master_df(m_path, sel_client)
                            merged = pd.concat([existing, df_mi], ignore_index=True).drop_duplicates(subset=["Indeed企業名"])
                            save_master_df(merged, m_path, sel_client)
                            st.success(f"✅ 追記しました（計{len(merged)}件）")
                            st.rerun()
                    with col_b:
                        if st.button("🔄 上書き（全件置き換え）", key="master_replace"):
                            save_master_df(df_mi, m_path, sel_client)
                            st.success(f"✅ 上書きしました（{len(df_mi)}件）")
                            st.rerun()

            else:  # IndeedのCSVから企業名を抽出
                st.caption("IndeedのCSVをアップロードすると、企業名の一覧が抽出されます。正規化名・大カテゴリ・業態を入力して保存してください。")
                indeed_import = st.file_uploader("IndeedのCSVをアップロード", type=["csv"], key="import_indeed_csv")
                if indeed_import:
                    df_indeed = pd.read_csv(indeed_import, encoding="utf-8-sig")
                    unique_names = sorted(df_indeed["企業名"].dropna().unique()) if "企業名" in df_indeed.columns else []
                    if not unique_names:
                        st.error("❌ 「企業名」列が見つかりません。IndeedのCSVか確認してください。")
                    else:
                        existing = load_master_df(m_path, sel_client)
                        registered = set(existing["Indeed企業名"].tolist())
                        new_names = [n for n in unique_names if n not in registered]

                        st.info(f"企業名 {len(unique_names)}社中、未登録: **{len(new_names)}社**")

                        # 市区町村列からエリアを企業名ごとに取得（最初の出現値）
                        area_map = {}
                        if "市区町村" in df_indeed.columns:
                            area_map = (
                                df_indeed.dropna(subset=["企業名", "市区町村"])
                                .groupby("企業名")["市区町村"]
                                .first()
                                .to_dict()
                            )

                        if new_names:
                            df_new = pd.DataFrame({
                                "Indeed企業名":              new_names,
                                "正規化名":                  ["" for _ in new_names],
                                "大カテゴリ":                ["" for _ in new_names],
                                "業態":                      ["" for _ in new_names],
                                "エリア":                    [area_map.get(n, "") for n in new_names],
                                "最寄り駅":                  ["" for _ in new_names],
                                "キーワード（カンマ区切り）": [n for n in new_names],
                            })

                            # AI推測ボタン
                            if st.button("🤖 AIで正規化名・大カテゴリ・業態を推測（Gemini）", key="ai_suggest"):
                                with st.spinner("Geminiが推測中..."):
                                    try:
                                        suggestions = suggest_store_labels(new_names)
                                        df_new["正規化名"]  = df_new["Indeed企業名"].map(lambda x: suggestions.get(x, {}).get("normalized", ""))
                                        df_new["大カテゴリ"] = df_new["Indeed企業名"].map(lambda x: suggestions.get(x, {}).get("category", ""))
                                        df_new["業態"]      = df_new["Indeed企業名"].map(lambda x: suggestions.get(x, {}).get("genre", ""))
                                        st.session_state["ai_suggested_df"] = df_new.copy()
                                        st.success("✅ 推測完了！内容を確認・修正してから保存してください。")
                                    except Exception as e:
                                        st.error(f"❌ AI推測エラー: {e}")

                            # AI推測後の結果があればそちらを使用
                            if "ai_suggested_df" in st.session_state:
                                df_new = st.session_state["ai_suggested_df"]

                            edited_new = st.data_editor(
                                df_new,
                                hide_index=True,
                                column_config={
                                    "Indeed企業名":          st.column_config.TextColumn("Indeed企業名", width="large"),
                                    "正規化名":              st.column_config.TextColumn("正規化名（レポートに表示する名前）", width="medium"),
                                    "大カテゴリ":            st.column_config.SelectboxColumn(
                                        "大カテゴリ", width="small",
                                        options=["", "飲食", "美容", "小売", "医療・介護", "その他"],
                                    ),
                                    "業態":                  st.column_config.SelectboxColumn(
                                        "業態", width="medium",
                                        options=GENRE_OPTIONS,
                                    ),
                                    "エリア":                st.column_config.TextColumn("エリア（例：新宿・渋谷）", width="medium"),
                                    "最寄り駅":              st.column_config.TextColumn("最寄り駅（例：新宿駅）", width="medium"),
                                    "キーワード（カンマ区切り）": st.column_config.TextColumn("キーワード（カンマ区切り）", width="large"),
                                },
                            )
                            st.caption("正規化名を入力してから保存してください。空白の行は保存されません。")

                            # 最寄り駅が空の行のGoogleマップリンクを事前表示
                            _no_station = df_new[df_new["最寄り駅"].fillna("").str.strip() == ""]
                            if not _no_station.empty:
                                with st.expander(f"🗺️ 最寄り駅が未入力の店舗 {len(_no_station)}件 ── 保存前にGoogleマップで確認できます"):
                                    st.caption("最寄り駅はデータ倉庫の分析に必要です。下のリンクでGoogleマップを開いて確認してから、上の表の「最寄り駅」列に入力してください。")
                                    for _, _row in _no_station.iterrows():
                                        _name = str(_row["正規化名"]).strip() or str(_row["Indeed企業名"]).strip()
                                        _area = str(_row.get("エリア", "")).strip()
                                        _query = urllib.parse.quote(f"{_name} {_area}".strip())
                                        _maps_url = f"https://www.google.com/maps/search/{_query}"
                                        st.markdown(f"- **{_name}**（{_area}） → [Googleマップで検索]({_maps_url})")

                            if st.button("💾 マスターに追加", key="master_from_indeed"):
                                to_add = edited_new[edited_new["正規化名"].str.strip() != ""]
                                if to_add.empty:
                                    st.warning("正規化名が入力されていません。")
                                else:
                                    merged = pd.concat([existing, to_add], ignore_index=True).drop_duplicates(subset=["Indeed企業名"])
                                    save_master_df(merged, m_path, sel_client)
                                    _saved_no_station = to_add[to_add["最寄り駅"].fillna("").str.strip() == ""]
                                    st.success(f"✅ {len(to_add)}社を追加しました（計{len(merged)}件）")
                                    if not _saved_no_station.empty:
                                        st.warning(f"⚠️ {len(_saved_no_station)}社の最寄り駅が未入力です。設定タブの「🗂 店舗マスター」から後で追加できます。")
                                    st.rerun()
                        else:
                            st.success("✅ すべての企業名が既にマスターに登録されています")

# ============================================================
# 📊 レポートタブ
# ============================================================
with main_tab:
    st.caption("CSVをアップロードして書き込みボタンを押すだけで完了します")
    st.divider()

    # ─── ① CSV アップロード ───────────────────────────────────
    st.subheader("① CSVをアップロード")
    uploaded_file = st.file_uploader(
        "IndeedからダウンロードしたCSVをここにドラッグ&ドロップ（JobsCampaigns_YYYYMMDD_YYYYMMDD.csv）",
        type=["csv"],
    )

    # ─── ② クライアント選択 ──────────────────────────────────
    st.subheader("② クライアントを選択")
    CLIENTS = load_clients()
    if not CLIENTS:
        st.warning("クライアントが登録されていません。「⚙️ 設定」タブで追加してください。")
        st.stop()

    client_name = st.selectbox("クライアント名", list(CLIENTS.keys()))

    if not uploaded_file:
        st.info("⬆️ まずCSVファイルをアップロードしてください")
        st.stop()

    # 期間取得
    period_start, period_end = parse_period_from_filename(uploaded_file.name)
    if not period_start:
        st.error("❌ ファイル名から期間を取得できませんでした。ファイル名に YYYYMMDD_YYYYMMDD が含まれているか確認してください。")
        st.stop()

    st.success(f"📅 集計期間: {period_start} 〜 {period_end}")

    # CSVパース
    content = uploaded_file.read().decode("utf-8-sig")
    rows = list(csv.DictReader(io.StringIO(content)))
    st.caption(f"読み込み: {len(rows)}行")

    # 集計（マスターはSheetsから直接取得）
    config = CLIENTS[client_name]
    master_df = load_master_df(config["master_path"], client_name)
    master = master_df_to_list(master_df)
    if not master:
        st.warning(
            f"⚠️ {client_name} の店舗マスターが見つかりません。"
            "「⚙️ 設定」→「🗂 店舗マスター」タブで店舗を登録してください。"
        )
        st.stop()
    rules = get_rules()

    data1, unmatched = aggregate(rows, master)
    data2, _         = aggregate_detail(rows, master, rules)
    sheet_rows1  = build_rows(data1, period_start, period_end)
    sheet_rows2  = build_rows_detail(data2, period_start, period_end)
    unknown_rows = build_rows_unknown(rows, master, period_start, period_end, rules)

    st.divider()

    # ─── ③ プレビュー ─────────────────────────────────────────
    st.subheader("③ 集計結果プレビュー")

    if unmatched:
        with st.expander(f"⚠️ マスター未登録の企業 {len(unmatched)}件（集計から除外されています）"):
            for name in sorted(unmatched):
                st.text(f"  ・{name}")
            st.caption("「⚙️ 設定」→「🗂 店舗マスター」タブで追加できます")

    tab1, tab2, tab3 = st.tabs(["① 店舗 × 雇用形態", "② 店舗 × 雇用形態 × 職種", "⚠️ 雇用形態不明"])

    with tab1:
        if sheet_rows1:
            df1 = pd.DataFrame(sheet_rows1, columns=COLS)
            st.dataframe(df1, hide_index=True)
            st.caption(f"{len(sheet_rows1)}行")
        else:
            st.info("書き込むデータがありません（すべて未マッチ）")

    with tab2:
        if sheet_rows2:
            df2 = pd.DataFrame(sheet_rows2, columns=COLS)
            st.dataframe(df2, hide_index=True)
            st.caption(f"{len(sheet_rows2)}行")
        else:
            st.info("書き込むデータがありません（すべて未マッチ）")

    with tab3:
        if unknown_rows:
            df3 = pd.DataFrame(unknown_rows, columns=COLS)
            st.dataframe(df3, hide_index=True)
            st.caption(f"{len(unknown_rows)}行（両シートの末尾に追記されます）")
        else:
            st.info("雇用形態不明の行はありません")

    st.divider()

    # ─── ④ 書き込み ──────────────────────────────────────────
    st.subheader("④ スプレッドシートに書き込む")

    if not (sheet_rows1 or sheet_rows2 or unknown_rows):
        st.warning("書き込めるデータがありません。クライアントのマスターCSVを確認してください。")
        st.stop()

    # 最寄り駅 未入力の店舗があれば警告（書き込みはブロックしない）
    _stores_in_data = {e["short_name"] for e in master}
    _missing_station_write = master_df[
        master_df["正規化名"].isin(_stores_in_data) &
        (master_df["最寄り駅"].fillna("").str.strip() == "")
    ]
    if not _missing_station_write.empty:
        _missing_names = "、".join(_missing_station_write["正規化名"].tolist())
        st.warning(
            f"⚠️ **最寄り駅が未入力の店舗があります（{len(_missing_station_write)}件）**: {_missing_names}\n\n"
            "このまま書き込めますが、データ倉庫に最寄り駅が記録されません。"
            "「⚙️ 設定 → 🗂 店舗マスター」から後で入力できます。"
        )

    with st.expander("🔍 キャッチコピー・写真取得（実験的機能）"):
        st.caption(
            "Indeed求人ページからキャッチコピーと写真説明を取得します。"
            "IndeedのBot検知により取得できない場合があります（その場合は空欄になります）。"
            "取得できなくても、求人URLとタイトルはデータ倉庫に記録されます。"
        )
        do_scrape = st.checkbox(
            "キャッチコピー・写真も取得する（1求人あたり約5〜10秒かかります）",
            value=False,
        )

    if st.button("🚀 スプレッドシートに書き込む", type="primary"):
        # スクレイピングはボタン押下後・メイン書き込み前に実行
        scraped_data = {}
        if do_scrape and rows:
            st.info(f"🔍 {len(set(r.get('求人URL','') for r in rows if r.get('求人URL')))}件の求人ページからキャッチコピー・写真を取得します...")
            scraped_data = batch_scrape(rows)
            got = sum(1 for v in scraped_data.values() if v[0])
            st.success(f"✅ キャッチコピー取得完了（{got}/{len(scraped_data)}件）")

        with st.spinner("書き込み中..."):
            try:
                service = get_service()

                if sheet_rows1:
                    deleted1 = delete_period_rows(service, config["spreadsheet_id"], config["sheet_pattern1"], period_start, period_end)
                    last1 = get_last_row(service, config["spreadsheet_id"], config["sheet_pattern1"])
                    append_to_sheet(service, config["spreadsheet_id"], config["sheet_pattern1"], sheet_rows1, last1 + 1)
                    msg1 = f"① {len(sheet_rows1)}行を書き込みました"
                    if deleted1:
                        msg1 += f"（既存{deleted1}行を上書き）"
                    st.success(f"✅ {msg1}")

                sheet2 = config.get("sheet_pattern2", "")
                if sheet2 and sheet_rows2:
                    deleted2 = delete_period_rows(service, config["spreadsheet_id"], sheet2, period_start, period_end)
                    last2 = get_last_row(service, config["spreadsheet_id"], sheet2)
                    append_to_sheet(service, config["spreadsheet_id"], sheet2, sheet_rows2, last2 + 1)
                    msg2 = f"② {len(sheet_rows2)}行を書き込みました"
                    if deleted2:
                        msg2 += f"（既存{deleted2}行を上書き）"
                    st.success(f"✅ {msg2}")
                elif not sheet2 and sheet_rows2:
                    st.info("ℹ️ シート②が未設定のため、詳細集計は書き込みをスキップしました")

                if unknown_rows:
                    target_sheets = [config["sheet_pattern1"]]
                    if sheet2:
                        target_sheets.append(sheet2)
                    for sname in target_sheets:
                        last = get_last_row(service, config["spreadsheet_id"], sname)
                        append_to_sheet(service, config["spreadsheet_id"], sname, unknown_rows, last + 1)
                    label = "①②両シート" if sheet2 else "シート①"
                    st.success(f"✅ 不明行 {len(unknown_rows)}行を{label}に追記しました")

                # ── データ倉庫に追記 ──────────────────────────
                if rows:
                    warehouse_rows = build_warehouse_rows(
                        client_name, rows, master, rules,
                        period_start, period_end, scraped=scraped_data
                    )
                    append_to_warehouse(service, warehouse_rows)
                    st.success(f"✅ データ倉庫に {len(warehouse_rows)}行を追記しました")

                st.link_button(
                    "📄 スプレッドシートを開く",
                    f"https://docs.google.com/spreadsheets/d/{config['spreadsheet_id']}",
                )
                st.link_button(
                    "🗄️ データ倉庫を開く",
                    f"https://docs.google.com/spreadsheets/d/{WAREHOUSE_SPREADSHEET_ID}",
                )

            except Exception as e:
                st.error(f"❌ エラーが発生しました: {e}")
