# -*- coding: utf-8 -*-
"""
Indeed レポート Streamlit UI
起動: streamlit run app.py  （または 起動.bat をダブルクリック）
"""

import csv
import io
import json
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
    load_store_master,
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
WAREHOUSE_COLS = [
    "取込日", "クライアント", "店舗", "大カテゴリ", "業態",
    "職種", "雇用形態", "集計開始", "集計終了",
    "表示回数", "クリック数", "応募開始数", "応募数", "費用",
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

# ============================================================
# 設定ファイルの読み書き
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


def load_clients():
    if CLIENTS_PATH.exists():
        return json.loads(CLIENTS_PATH.read_text(encoding="utf-8"))
    return _DEFAULT_CLIENTS


def save_clients(clients: dict):
    CLIENTS_PATH.write_text(json.dumps(clients, ensure_ascii=False, indent=2), encoding="utf-8")


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


def load_master_df(master_path: str) -> pd.DataFrame:
    p = BASE_DIR / master_path
    empty = pd.DataFrame(columns=["Indeed企業名", "正規化名", "大カテゴリ", "業態", "キーワード（カンマ区切り）"])
    if not p.exists():
        return empty
    with open(p, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return empty
    return pd.DataFrame([
        {
            "Indeed企業名":          r["store_name"],
            "正規化名":              r["short_name"],
            "大カテゴリ":            r.get("category", ""),
            "業態":                  r.get("genre", ""),
            "キーワード（カンマ区切り）": r["keywords"],
        }
        for r in rows
    ])


def save_master_df(df: pd.DataFrame, master_path: str):
    p = BASE_DIR / master_path
    p.parent.mkdir(exist_ok=True)
    out = df.rename(columns={
        "Indeed企業名":          "store_name",
        "正規化名":              "short_name",
        "大カテゴリ":            "category",
        "業態":                  "genre",
        "キーワード（カンマ区切り）": "keywords",
    })
    # 列順を固定
    cols = [c for c in ["store_name", "short_name", "category", "genre", "keywords"] if c in out.columns]
    out[cols].to_csv(p, index=False, encoding="utf-8-sig")


def get_rules():
    return load_job_role_rules(str(RULES_PATH)) if RULES_PATH.exists() else None


# ============================================================
# Sheets ヘルパー
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
    """「データ倉庫」シートがなければ作成し、ヘッダーも追加する"""
    spreadsheet = service.spreadsheets().get(spreadsheetId=WAREHOUSE_SPREADSHEET_ID).execute()
    exists = any(s["properties"]["title"] == WAREHOUSE_SHEET_NAME for s in spreadsheet["sheets"])
    if not exists:
        service.spreadsheets().batchUpdate(
            spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": WAREHOUSE_SHEET_NAME}}}]},
        ).execute()

    # ヘッダー行がなければ追加
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


def build_warehouse_rows(client_name, data2, master, period_start, period_end):
    """データ倉庫用の行を生成（詳細データ: 店舗×雇用形態×職種）"""
    today = date.today().strftime("%Y/%m/%d")
    store_labels = {e["short_name"]: (e.get("category", ""), e.get("genre", "")) for e in master}

    rows = []
    for (short_name, emp_type, job_title) in sorted(data2.keys()):
        d = data2[(short_name, emp_type, job_title)]
        cat, genre = store_labels.get(short_name, ("", ""))
        rows.append([
            today, client_name, short_name, cat, genre,
            job_title, emp_type, period_start, period_end,
            d["表示回数"], d["クリック数"], d["応募開始数"], d["応募数"],
            round(d["費用"]),
        ])
    return rows


def append_to_warehouse(service, warehouse_rows):
    ensure_warehouse_sheet(service)
    last = get_last_row(service, WAREHOUSE_SPREADSHEET_ID, WAREHOUSE_SHEET_NAME)
    next_row = last + 1
    range_str = f"'{WAREHOUSE_SHEET_NAME}'!A{next_row}:N{next_row + len(warehouse_rows) - 1}"
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
            use_container_width=True,
            hide_index=True,
            column_config={
                "クライアント名":    st.column_config.TextColumn("クライアント名", width="small"),
                "スプレッドシートID": st.column_config.TextColumn("スプレッドシートID", width="large"),
                "シート名①":         st.column_config.TextColumn("シート名①（集計）", width="medium"),
                "シート名②":         st.column_config.TextColumn("シート名②（詳細・省略可）", width="medium"),
            },
        )
        st.caption("💡 店舗マスターファイルは `masters/クライアント名.csv` に自動保存されます。「🗂 店舗マスター」タブで編集できます。")

        if st.button("💾 クライアントを保存", key="save_clients"):
            save_clients(df_to_clients(edited_clients_df))
            st.success("✅ 保存しました")
            st.rerun()

    # ─── 職種ルール ──────────────────────────────────────────
    with s2:
        st.subheader("職種の正規化ルール")
        st.caption("**上から順に評価**されます。先にマッチしたルールが優先されます。")

        edited_rules_df = st.data_editor(
            load_rules_df(),
            num_rows="dynamic",
            use_container_width=True,
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
            st.dataframe(df_ri, use_container_width=True, hide_index=True)
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

            edited_master_df = st.data_editor(
                load_master_df(m_path),
                num_rows="dynamic",
                use_container_width=True,
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
                    "キーワード（カンマ区切り）": st.column_config.TextColumn("マッチキーワード（カンマ区切り）", width="large"),
                },
            )
            st.caption("💡 大カテゴリ・業態はデータ倉庫の分析に使われます。キーワードが**すべて**含まれていれば一致します。")

            if st.button("💾 マスターを保存", key="save_master"):
                save_master_df(edited_master_df, m_path)
                st.success(f"✅ {m_path} を保存しました")
                st.rerun()

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
                    st.dataframe(df_mi, use_container_width=True, hide_index=True)
                    st.caption(f"{len(df_mi)}行を読み込みました")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("➕ 既存に追記", key="master_append"):
                            existing = load_master_df(m_path)
                            merged = pd.concat([existing, df_mi], ignore_index=True).drop_duplicates(subset=["Indeed企業名"])
                            save_master_df(merged, m_path)
                            st.success(f"✅ 追記しました（計{len(merged)}件）")
                            st.rerun()
                    with col_b:
                        if st.button("🔄 上書き（全件置き換え）", key="master_replace"):
                            save_master_df(df_mi, m_path)
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
                        existing = load_master_df(m_path)
                        registered = set(existing["Indeed企業名"].tolist())
                        new_names = [n for n in unique_names if n not in registered]

                        st.info(f"企業名 {len(unique_names)}社中、未登録: **{len(new_names)}社**")

                        if new_names:
                            df_new = pd.DataFrame({
                                "Indeed企業名": new_names,
                                "正規化名": ["" for _ in new_names],
                                "大カテゴリ": ["" for _ in new_names],
                                "業態": ["" for _ in new_names],
                                "キーワード（カンマ区切り）": [n for n in new_names],
                            })
                            edited_new = st.data_editor(
                                df_new,
                                use_container_width=True,
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
                                    "キーワード（カンマ区切り）": st.column_config.TextColumn("キーワード（カンマ区切り）", width="large"),
                                },
                            )
                            st.caption("正規化名を入力してから保存してください。空白の行は保存されません。")
                            if st.button("💾 マスターに追加", key="master_from_indeed"):
                                to_add = edited_new[edited_new["正規化名"].str.strip() != ""]
                                if to_add.empty:
                                    st.warning("正規化名が入力されていません。")
                                else:
                                    merged = pd.concat([existing, to_add], ignore_index=True).drop_duplicates(subset=["Indeed企業名"])
                                    save_master_df(merged, m_path)
                                    st.success(f"✅ {len(to_add)}社を追加しました（計{len(merged)}件）")
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

    # 集計
    config = CLIENTS[client_name]
    master = load_store_master(config["master_path"])
    if not master:
        st.warning(
            f"⚠️ `{config['master_path']}` が見つかりません。"
            "「⚙️ 設定」→「🗂 店舗マスター」タブで店舗を登録してください。"
        )
        st.stop()
    rules  = get_rules()

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
            st.dataframe(df1, use_container_width=True, hide_index=True)
            st.caption(f"{len(sheet_rows1)}行")
        else:
            st.info("書き込むデータがありません（すべて未マッチ）")

    with tab2:
        if sheet_rows2:
            df2 = pd.DataFrame(sheet_rows2, columns=COLS)
            st.dataframe(df2, use_container_width=True, hide_index=True)
            st.caption(f"{len(sheet_rows2)}行")
        else:
            st.info("書き込むデータがありません（すべて未マッチ）")

    with tab3:
        if unknown_rows:
            df3 = pd.DataFrame(unknown_rows, columns=COLS)
            st.dataframe(df3, use_container_width=True, hide_index=True)
            st.caption(f"{len(unknown_rows)}行（両シートの末尾に追記されます）")
        else:
            st.info("雇用形態不明の行はありません")

    st.divider()

    # ─── ④ 書き込み ──────────────────────────────────────────
    st.subheader("④ スプレッドシートに書き込む")

    if not (sheet_rows1 or sheet_rows2 or unknown_rows):
        st.warning("書き込めるデータがありません。クライアントのマスターCSVを確認してください。")
        st.stop()

    if st.button("🚀 スプレッドシートに書き込む", type="primary"):
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
                if data2:
                    warehouse_rows = build_warehouse_rows(client_name, data2, master, period_start, period_end)
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
