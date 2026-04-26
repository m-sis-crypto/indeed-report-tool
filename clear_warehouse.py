# -*- coding: utf-8 -*-
"""
データ倉庫のデータ行を一括クリアするスクリプト

【使い方】
  py -X utf8 clear_warehouse.py

ヘッダー行（1行目）は残し、2行目以降のデータを全削除します。
"""

from pathlib import Path
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

WAREHOUSE_SPREADSHEET_ID = "1Vr7-IpCgEG4Gl2kRhb86Gxz4vYg8R9VpkTEpLNzvuF4"
WAREHOUSE_SHEET_NAME = "データ倉庫"
TOKEN_PATH = Path(r"C:\Users\mgm03\OneDrive\デスクトップ\AIエージェント\tabelog_tool\レビュワー取得\token.json")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def main():
    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    service = build("sheets", "v4", credentials=creds)

    # 現在の行数を確認
    result = service.spreadsheets().values().get(
        spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
        range=f"'{WAREHOUSE_SHEET_NAME}'!A:A",
    ).execute()
    rows = result.get("values", [])
    data_count = len(rows) - 1  # ヘッダー除く
    print(f"現在のデータ行数: {data_count}件")

    if data_count <= 0:
        print("削除するデータがありません")
        return

    confirm = input(f"\n⚠️  {data_count}件のデータを削除します。よろしいですか？ (y/N): ")
    if confirm.lower() != "y":
        print("キャンセルしました")
        return

    service.spreadsheets().values().clear(
        spreadsheetId=WAREHOUSE_SPREADSHEET_ID,
        range=f"'{WAREHOUSE_SHEET_NAME}'!A2:Z",
    ).execute()
    print(f"✅ {data_count}件のデータを削除しました（ヘッダー行は保持）")


if __name__ == "__main__":
    main()
