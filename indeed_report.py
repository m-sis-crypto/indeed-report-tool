# -*- coding: utf-8 -*-
"""
Indeed キャンペーンCSV 自動レポート生成ツール

使い方:
  py -3 indeed_report.py <CSVファイルパス> <マスターCSVパス>

例:
  py -3 indeed_report.py "C:/Users/mgm03/Downloads/JobsCampaigns_20260302_20260329.csv" masters/allstarted.csv

仕様:
  - 無料/有料キャンペーン区別なし（全行を集計）
  - 集計単位: 店舗 × 雇用形態
  - 期間: CSVファイル名の YYYYMMDD_YYYYMMDD から自動取得
  - 書き込み先: スプレッドシートの「レポート抽出」タブ末尾に追記
"""

import csv
import sys
import re
from datetime import datetime
from pathlib import Path
from collections import defaultdict

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ============================================================
# 設定
# ============================================================

TOKEN_PATH = Path(r"C:\Users\mgm03\OneDrive\デスクトップ\AIエージェント\tabelog_tool\レビュワー取得\token.json")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1bxHZxhPFzgoz-xXSk8b7Onk5G-N6KcwehAZMiqiJqM4'
SHEET_NAME        = 'レポート抽出'        # パターン①: 店舗×雇用形態
SHEET_NAME_DETAIL = 'レポート抽出_詳細'   # パターン②: 店舗×雇用形態×職種

# ============================================================
# 店舗マスター
# ============================================================

def load_store_master(master_path):
    p = Path(master_path)
    if not p.exists():
        return []
    master = []
    with open(p, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            keywords = [kw.strip() for kw in row['keywords'].split(',')]
            master.append({
                'store':           row['store_name'],
                'short_name':      row['short_name'],
                'keywords':        keywords,
                'category':        row.get('category', ''),
                'genre':           row.get('genre', ''),
                'area':            row.get('area', ''),
                'nearest_station': row.get('nearest_station', ''),
            })
    return master


def normalize_store(company_name, master):
    """キーワードが全部含まれていれば一致。short_nameを返す。"""
    for entry in master:
        if all(kw in company_name for kw in entry['keywords']):
            return entry['short_name']
    return None


# ============================================================
# 雇用形態の抽出
# ============================================================

def extract_employment_type(job_title, campaign=''):
    """求人列 → 不明の場合はキャンペーン列にフォールバック"""
    if '(正社員)' in job_title or '（正社員）' in job_title:
        return '正社員'
    elif '(アルバイト)' in job_title or '（アルバイト）' in job_title or 'アルバイト' in job_title:
        return 'AP'
    # キャンペーン列で確認
    if '正社員' in campaign or '社員' in campaign:
        return '正社員'
    elif 'AP' in campaign or 'アルバイト' in campaign:
        return 'AP'
    return '不明'


# 職種正規化ルール（上から順に評価・先にマッチしたものが優先）
JOB_ROLE_RULES = [
    ('調理補助', ['調理補助', 'キッチン補助']),
    ('調理',     ['調理', 'キッチン']),
    ('ホール',   ['ホール']),
    ('店舗',     ['店舗']),
    ('深夜アルバイト', ['深夜アルバイト']),
    ('アルバイト', ['アルバイト']),
]


def load_job_role_rules(path):
    """職種正規化ルールをCSVから読み込む"""
    rules = []
    with open(path, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            keywords = [kw.strip() for kw in row['keywords'].split(',')]
            rules.append((row['canonical'], keywords))
    return rules


def normalize_job_role(core, rules=None):
    if rules is None:
        rules = JOB_ROLE_RULES
    for canonical, keywords in rules:
        if any(kw in core for kw in keywords):
            return canonical
    return core


def extract_job_title(job_full, rules=None):
    """求人列からコア職種を抽出して正規化する"""
    stripped = re.sub(r'\s*[（(][^）)]*[）)]\s*$', '', job_full).strip()
    idx = stripped.rfind('の')
    core = stripped[idx + 1:].strip() if idx != -1 else stripped
    return normalize_job_role(core, rules)


# ============================================================
# 期間をファイル名から取得
# ============================================================

def parse_period_from_filename(filepath):
    """
    ファイル名の YYYYMMDD_YYYYMMDD パターンから掲載開始・終了を取得。
    例: JobsCampaigns_20260302_20260329 → ('2026年3月2日', '2026年3月29日')
    """
    name = Path(filepath).stem  # 拡張子・括弧を除いたファイル名
    m = re.search(r'(\d{8})_(\d{8})', name)
    if not m:
        return None, None

    def fmt(s):
        d = datetime.strptime(s, '%Y%m%d')
        return f"{d.year}年{d.month}月{d.day}日"

    return fmt(m.group(1)), fmt(m.group(2))


# ============================================================
# CSV 読み込み・集計
# ============================================================

def load_csv(filepath):
    with open(filepath, encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def to_int(val):
    try:
        return int(str(val).replace(',', '') or 0)
    except ValueError:
        return 0


def to_float(val):
    try:
        return float(str(val).replace(',', '') or 0)
    except ValueError:
        return 0.0


def aggregate(rows, master):
    """パターン①: 店舗 × 雇用形態 で集計（不明は除外）"""
    data = defaultdict(lambda: {
        '表示回数': 0, 'クリック数': 0, '応募開始数': 0, '応募数': 0, '費用': 0.0
    })
    unmatched = set()

    for row in rows:
        short_name = normalize_store(row['企業名'], master)
        if not short_name:
            unmatched.add(row['企業名'])
            continue

        emp_type = extract_employment_type(row['求人'], row.get('キャンペーン', ''))
        if emp_type == '不明':
            continue  # 不明は別途 build_rows_unknown で出力
        key = (short_name, emp_type)

        data[key]['表示回数']  += to_int(row['表示回数'])
        data[key]['クリック数'] += to_int(row['クリック数'])
        data[key]['応募開始数'] += to_int(row['応募開始数'])
        data[key]['応募数']    += to_int(row['応募数'])
        data[key]['費用']      += to_float(row['費用'])

    return data, unmatched


def aggregate_detail(rows, master, rules=None):
    """パターン②: 店舗 × 雇用形態 × 職種 で集計（不明は除外）"""
    data = defaultdict(lambda: {
        '表示回数': 0, 'クリック数': 0, '応募開始数': 0, '応募数': 0, '費用': 0.0
    })
    unmatched = set()

    for row in rows:
        short_name = normalize_store(row['企業名'], master)
        if not short_name:
            unmatched.add(row['企業名'])
            continue

        emp_type  = extract_employment_type(row['求人'], row.get('キャンペーン', ''))
        if emp_type == '不明':
            continue  # 不明は別途 build_rows_unknown で出力
        job_title = extract_job_title(row['求人'], rules)
        key = (short_name, emp_type, job_title)

        data[key]['表示回数']  += to_int(row['表示回数'])
        data[key]['クリック数'] += to_int(row['クリック数'])
        data[key]['応募開始数'] += to_int(row['応募開始数'])
        data[key]['応募数']    += to_int(row['応募数'])
        data[key]['費用']      += to_float(row['費用'])

    return data, unmatched


# ============================================================
# スプレッドシート書き込み
# ============================================================

def get_sheets_service():
    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build('sheets', 'v4', credentials=creds)


def get_last_row(service, sheet_name):
    """既存データの最終行番号を取得"""
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{sheet_name}'!A:A"
    ).execute()
    return len(result.get('values', []))


def get_sheet_id_num(service, sheet_name):
    """シート名から数値IDを取得（行削除APIに必要）"""
    spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == sheet_name:
            return sheet['properties']['sheetId']
    raise ValueError(f'シートが見つかりません: {sheet_name}')


def delete_period_rows(service, sheet_name, period_start, period_end):
    """同一期間（D列=開始・E列=終了）の既存行を削除。削除行数を返す。"""
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{sheet_name}'!D:E"
    ).execute()
    values = result.get('values', [])

    # 削除対象の行インデックス（0始まり）を収集
    rows_to_delete = [
        i for i, row in enumerate(values)
        if len(row) >= 2 and row[0] == period_start and row[1] == period_end
    ]
    if not rows_to_delete:
        return 0

    sheet_id_num = get_sheet_id_num(service, sheet_name)
    # 逆順で削除リクエストを作成（削除後の行番号ずれを防ぐ）
    requests = [
        {
            'deleteDimension': {
                'range': {
                    'sheetId': sheet_id_num,
                    'dimension': 'ROWS',
                    'startIndex': row_idx,
                    'endIndex': row_idx + 1,
                }
            }
        }
        for row_idx in sorted(rows_to_delete, reverse=True)
    ]
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={'requests': requests}
    ).execute()
    return len(rows_to_delete)


def build_rows(data, period_start, period_end):
    """パターン①: A〜K列（K列=参照番号は空）"""
    rows = []
    for (short_name, emp_type) in sorted(data.keys()):
        d = data[(short_name, emp_type)]
        rows.append([
            short_name,               # A: 店舗
            '',                       # B: 職種（空）
            emp_type,                 # C: 雇用形態
            period_start,             # D: 掲載開始
            period_end,               # E: 掲載終了
            d['表示回数'],              # F: 表示回数
            d['クリック数'],             # G: クリック数
            d['応募開始数'],             # H: 応募開始数
            d['応募数'],                # I: 応募数
            f"¥{round(d['費用']):,}",  # J: 費用
            '',                       # K: 参照番号（集計行は空）
        ])
    return rows


def build_rows_detail(data, period_start, period_end):
    """パターン②: A〜K列（K列=参照番号は空）"""
    rows = []
    for (short_name, emp_type, job_title) in sorted(data.keys()):
        d = data[(short_name, emp_type, job_title)]
        rows.append([
            short_name,               # A: 店舗
            job_title,                # B: 職種
            emp_type,                 # C: 雇用形態
            period_start,             # D: 掲載開始
            period_end,               # E: 掲載終了
            d['表示回数'],              # F: 表示回数
            d['クリック数'],             # G: クリック数
            d['応募開始数'],             # H: 応募開始数
            d['応募数'],                # I: 応募数
            f"¥{round(d['費用']):,}",  # J: 費用
            '',                       # K: 参照番号（集計行は空）
        ])
    return rows


def build_rows_unknown(rows, master, period_start, period_end, rules=None):
    """雇用形態不明行を個別出力（集計なし・K列=参照番号付き）"""
    result = []
    for row in rows:
        short_name = normalize_store(row['企業名'], master)
        if not short_name:
            continue
        emp_type = extract_employment_type(row['求人'], row.get('キャンペーン', ''))
        if emp_type != '不明':
            continue
        result.append([
            short_name,                                # A: 店舗
            extract_job_title(row['求人'], rules),     # B: 職種
            '不明',                               # C: 雇用形態
            period_start,                         # D: 掲載開始
            period_end,                           # E: 掲載終了
            to_int(row['表示回数']),                # F: 表示回数
            to_int(row['クリック数']),               # G: クリック数
            to_int(row['応募開始数']),               # H: 応募開始数
            to_int(row['応募数']),                  # I: 応募数
            f"¥{round(to_float(row['費用'])):,}",  # J: 費用
            row.get('参照番号', ''),               # K: 参照番号
        ])
    return result


def ensure_sheet_rows(service, sheet_name, required_row):
    """シートの行数が足りない場合に末尾へ行を追加する"""
    spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id_num = None
    current_rows = 0
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == sheet_name:
            sheet_id_num = sheet['properties']['sheetId']
            current_rows = sheet['properties']['gridProperties']['rowCount']
            break
    if sheet_id_num is None or current_rows >= required_row:
        return
    append_count = required_row - current_rows + 50  # 少し余裕を持たせる
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={'requests': [{
            'appendDimension': {
                'sheetId': sheet_id_num,
                'dimension': 'ROWS',
                'length': append_count,
            }
        }]}
    ).execute()


def append_to_sheet(service, rows, next_row, sheet_name):
    """指定行から追記（既存データを壊さない）"""
    ensure_sheet_rows(service, sheet_name, next_row + len(rows) - 1)
    range_str = f"'{sheet_name}'!A{next_row}:K{next_row + len(rows) - 1}"
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=range_str,
        valueInputOption='RAW',
        body={'values': rows}
    ).execute()


# ============================================================
# コンソール表示
# ============================================================

def print_summary(data, period_start, period_end, unmatched):
    print()
    print(f'【パターン①: 店舗 × 雇用形態】  期間: {period_start} 〜 {period_end}')
    print('=' * 72)
    print(f"{'店舗':<12} {'雇用形態':<6} {'表示回数':>8} {'クリック':>8} {'応募開始':>8} {'応募数':>6} {'費用':>12}")
    print('-' * 72)

    for (short_name, emp_type) in sorted(data.keys()):
        d = data[(short_name, emp_type)]
        print(
            f"{short_name:<12} {emp_type:<6} "
            f"{d['表示回数']:>8,} {d['クリック数']:>8,} {d['応募開始数']:>8,} "
            f"{d['応募数']:>6,} ¥{round(d['費用']):>11,}"
        )

    if unmatched:
        print()
        print(f'未マッチ企業名 {len(unmatched)}件:')
        for name in sorted(unmatched):
            print(f'  - {name}')
    else:
        print()
        print('未マッチ: 0件')


def print_summary_detail(data, period_start, period_end):
    print()
    print(f'【パターン②: 店舗 × 雇用形態 × 職種】  期間: {period_start} 〜 {period_end}')
    print('=' * 84)
    print(f"{'店舗':<12} {'雇用形態':<6} {'職種':<16} {'表示回数':>8} {'クリック':>8} {'応募開始':>8} {'応募数':>6} {'費用':>12}")
    print('-' * 84)

    for (short_name, emp_type, job_title) in sorted(data.keys()):
        d = data[(short_name, emp_type, job_title)]
        print(
            f"{short_name:<12} {emp_type:<6} {job_title:<16} "
            f"{d['表示回数']:>8,} {d['クリック数']:>8,} {d['応募開始数']:>8,} "
            f"{d['応募数']:>6,} ¥{round(d['費用']):>11,}"
        )


# ============================================================
# メイン
# ============================================================

def main():
    sys.stdout.reconfigure(encoding='utf-8')

    if len(sys.argv) < 3:
        print('使い方: py -3 indeed_report.py <CSVファイルパス> <マスターCSVパス>')
        sys.exit(1)

    csv_path   = sys.argv[1]
    master_path = sys.argv[2]

    # 期間をファイル名から取得
    period_start, period_end = parse_period_from_filename(csv_path)
    if not period_start:
        print('[ERROR] ファイル名から期間を取得できませんでした。')
        print('  ファイル名に YYYYMMDD_YYYYMMDD が含まれている必要があります。')
        sys.exit(1)

    print(f'[CSV]    {Path(csv_path).name}')
    print(f'[マスター] {Path(master_path).name}')
    print(f'[期間]   {period_start} 〜 {period_end}')

    master = load_store_master(master_path)
    rows   = load_csv(csv_path)
    print(f'読み込み: {len(rows)}行 / マスター{len(master)}店舗')

    # ── 集計 ──
    data1, unmatched = aggregate(rows, master)
    data2, _         = aggregate_detail(rows, master)

    print_summary(data1, period_start, period_end, unmatched)
    print_summary_detail(data2, period_start, period_end)

    # ── スプレッドシートに追記 ──
    print('\nスプレッドシートに追記中...')
    service = get_sheets_service()

    # パターン①
    sheet_rows1 = build_rows(data1, period_start, period_end)
    if sheet_rows1:
        deleted1 = delete_period_rows(service, SHEET_NAME, period_start, period_end)
        if deleted1:
            print(f'  [①] 既存データ {deleted1}行を削除（上書き）')
        last_row1 = get_last_row(service, SHEET_NAME)
        next_row1 = last_row1 + 1
        print(f'  [①] "{SHEET_NAME}" 行{next_row1}〜{next_row1 + len(sheet_rows1) - 1}（{len(sheet_rows1)}行）')
        append_to_sheet(service, sheet_rows1, next_row1, SHEET_NAME)
    else:
        print(f'  [①] 書き込み対象なし（マスター未マッチ）')

    # パターン②
    sheet_rows2 = build_rows_detail(data2, period_start, period_end)
    if sheet_rows2:
        deleted2 = delete_period_rows(service, SHEET_NAME_DETAIL, period_start, period_end)
        if deleted2:
            print(f'  [②] 既存データ {deleted2}行を削除（上書き）')
        last_row2 = get_last_row(service, SHEET_NAME_DETAIL)
        next_row2 = last_row2 + 1
        print(f'  [②] "{SHEET_NAME_DETAIL}" 行{next_row2}〜{next_row2 + len(sheet_rows2) - 1}（{len(sheet_rows2)}行）')
        append_to_sheet(service, sheet_rows2, next_row2, SHEET_NAME_DETAIL)
    else:
        print(f'  [②] 書き込み対象なし（マスター未マッチ）')

    # 不明行（両シートの末尾に追記）
    unknown_rows = build_rows_unknown(rows, master, period_start, period_end)
    if unknown_rows:
        for sname in (SHEET_NAME, SHEET_NAME_DETAIL):
            last = get_last_row(service, sname)
            print(f'  [不明] "{sname}" 行{last + 1}〜{last + len(unknown_rows)}（{len(unknown_rows)}行）')
            append_to_sheet(service, unknown_rows, last + 1, sname)
    else:
        print('  [不明] 不明行なし')

    print(f'\n完了！ スプレッドシートを確認してください。')
    print(f'https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}')


if __name__ == '__main__':
    main()
