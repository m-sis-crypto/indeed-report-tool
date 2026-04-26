# -*- coding: utf-8 -*-
"""
データ倉庫にCSVを一括インポートするローカルスクリプト
（Streamlitアプリを経由せず直接実行できる）

【使い方】
  py -X utf8 import_warehouse.py
"""

import csv
import io
import sys
from datetime import date
from pathlib import Path

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

sys.path.insert(0, str(Path(__file__).parent))
from indeed_report import (
    normalize_store,
    extract_employment_type,
    extract_job_title,
    load_job_role_rules,
    parse_period_from_filename,
    to_int,
    to_float,
)

# ============================================================
# 設定
# ============================================================
WAREHOUSE_SPREADSHEET_ID = "1Vr7-IpCgEG4Gl2kRhb86Gxz4vYg8R9VpkTEpLNzvuF4"
WAREHOUSE_SHEET_NAME     = "データ倉庫"
CONFIG_SHEET_NAME        = "クライアント設定"
TOKEN_PATH = Path(r"C:\Users\mgm03\OneDrive\デスクトップ\AIエージェント\tabelog_tool\レビュワー取得\token.json")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
RULES_PATH = Path(__file__).parent / "config" / "job_role_rules.csv"

WAREHOUSE_COLS = [
    "取込日", "クライアント", "店舗", "大カテゴリ", "業態", "エリア", "最寄り駅",
    "職種", "雇用形態", "参照番号", "求人タイトル", "求人URL",
    "集計開始", "集計終了",
    "表示回数", "クリック数", "応募開始数", "応募数", "費用",
    "キャッチコピー", "写真説明", "給与",
]

# インポートするCSVとクライアント名のマッピング
CSV_TO_CLIENT = {
    r"C:\Users\mgm03\Downloads\JobsCampaigns_20260301_20260331 (2).csv": "TOU",
    r"C:\Users\mgm03\Downloads\JobsCampaigns_20260301_20260331 (3).csv": "KAI",
    r"C:\Users\mgm03\Downloads\JobsCampaigns_20260303_20260401 (1).csv": "ALLSTARTED",
    r"C:\Users\mgm03\Downloads\JobsCampaigns_20260301_20260331 (4).csv": "OTTO",
}


# ============================================================
# Google Sheets
# ============================================================
def get_service():
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


def ensure_warehouse_header(service):
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
        print("  ヘッダー行を書き込みました")


def load_master_from_sheets(service, client_name: str) -> list:
    """マスターシートから店舗一覧をindeed_report互換のlist形式で返す"""
    sheet_name = f"マスター_{client_name}"
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
            range=f"'{sheet_name}'!A:G"
        ).execute()
        values = result.get("values", [])
        if len(values) < 2:
            return []
        header = values[0]
        col = {h: i for i, h in enumerate(header)}

        def get(row, *keys):
            for k in keys:
                if k in col and col[k] < len(row):
                    return row[col[k]]
            return ""

        master = []
        for row in values[1:]:
            store_name = get(row, "store_name", "Indeed企業名")
            short_name = get(row, "short_name", "正規化名")
            if not store_name:
                continue
            keywords_raw = get(row, "keywords", "キーワード（カンマ区切り）")
            keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]
            master.append({
                "store":           store_name,
                "short_name":      short_name or store_name,
                "keywords":        keywords,
                "category":        get(row, "category", "大カテゴリ"),
                "genre":           get(row, "genre", "業態"),
                "area":            get(row, "area", "エリア"),
                "nearest_station": get(row, "nearest_station", "最寄り駅"),
            })
        return master
    except Exception as e:
        print(f"  ⚠️  マスター読み込みエラー（{sheet_name}）: {e}")
        return []


def append_to_warehouse(service, warehouse_rows: list):
    ensure_warehouse_header(service)
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
# 倉庫行の生成
# ============================================================
def build_warehouse_rows(client_name, rows_raw, master, rules, period_start, period_end):
    today = date.today().strftime("%Y/%m/%d")
    store_labels = {
        e["short_name"]: (
            e.get("category", ""), e.get("genre", ""),
            e.get("area", ""), e.get("nearest_station", ""),
        )
        for e in master
    }
    out = []
    unmatched = set()
    for row in rows_raw:
        short_name = normalize_store(row["企業名"], master)
        if not short_name:
            unmatched.add(row["企業名"])
            continue
        emp_type  = extract_employment_type(row["求人"], row.get("キャンペーン", ""))
        job_title = extract_job_title(row["求人"], rules)
        cat, genre, area, station = store_labels.get(short_name, ("", "", "", ""))
        url = row.get("求人URL", "")
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
            "",  # キャッチコピー（fetch_job_details.pyで取得）
            "",  # 写真説明
            "",  # 給与
        ])
    return out, unmatched


# ============================================================
# メイン処理
# ============================================================
def main():
    print("=" * 60)
    print("データ倉庫 CSV一括インポート")
    print("=" * 60)

    service = get_service()
    rules = load_job_role_rules(str(RULES_PATH)) if RULES_PATH.exists() else None

    total_imported = 0

    for csv_path, client_name in CSV_TO_CLIENT.items():
        p = Path(csv_path)
        if not p.exists():
            print(f"\n⚠️  ファイルが見つかりません: {p.name} → スキップ")
            continue

        print(f"\n{'─' * 50}")
        print(f"📥 {client_name} ← {p.name}")

        # CSVパース
        with open(p, encoding="utf-8-sig") as f:
            rows_raw = list(csv.DictReader(f))
        print(f"  CSV行数: {len(rows_raw)}行")

        # 期間
        period_start, period_end = parse_period_from_filename(p.name)
        print(f"  集計期間: {period_start} 〜 {period_end}")

        # マスター読み込み
        master = load_master_from_sheets(service, client_name)
        if not master:
            print(f"  ⚠️  マスターが空です → データ倉庫への書き込みをスキップ")
            print(f"     本番アプリの設定 → 店舗マスターでマスターを登録してください")
            continue
        print(f"  マスター: {len(master)}件")

        # 倉庫行の生成
        warehouse_rows, unmatched = build_warehouse_rows(
            client_name, rows_raw, master, rules, period_start, period_end
        )

        if unmatched:
            print(f"  ⚠️  マスター未登録（{len(unmatched)}社）:")
            for name in sorted(unmatched):
                print(f"     ・{name}")

        if not warehouse_rows:
            print(f"  書き込む行がありません（全行がマスター未登録）")
            continue

        # 倉庫に追記
        append_to_warehouse(service, warehouse_rows)
        print(f"  ✅ {len(warehouse_rows)}行をデータ倉庫に書き込みました")
        total_imported += len(warehouse_rows)

    print(f"\n{'=' * 60}")
    print(f"✅ 完了: 合計 {total_imported}行 をインポートしました")
    print(f"\n次のステップ: py -X utf8 fetch_job_details.py")
    print(f"  → キャッチコピー・写真説明・給与・エリア・最寄り駅を一括取得します")


if __name__ == "__main__":
    main()
