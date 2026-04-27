# -*- coding: utf-8 -*-
"""
スクレイピングのデバッグ用テストスクリプト
指定URLのページを開いて、エリア・最寄り駅・アクセスセクションの中身を全部表示する
"""
import re

URLS = [
    "http://jp.indeed.com/job/%E3%81%8A%E6%B4%92%E8%90%BD%E8%87%AA%E7%94%B1%E3%82%A4%E3%82%BF%E3%83%AA%E3%82%A2%E3%83%B3%E3%83%93%E3%82%B9%E3%83%88%E3%83%AD%E3%81%AE%E3%83%9B%E3%83%BC%E3%83%AB%E3%82%B9%E3%82%BF%E3%83%83%E3%83%95%E3%82%A2%E3%83%AB%E3%83%90%E3%82%A4%E3%83%88-43ea19ee215e5c88",
    "http://jp.indeed.com/job/%E3%82%AA%E3%82%B7%E3%83%A3%E3%83%AC%E8%87%AA%E7%94%B1%E3%81%AA%E3%82%A4%E3%82%BF%E3%83%AA%E3%82%A2%E3%83%B3%E3%83%90%E3%83%AB%E3%81%AE%E3%83%9B%E3%83%BC%E3%83%AB%E3%82%B9%E3%82%BF%E3%83%83%E3%83%95%E3%82%A2%E3%83%AB%E3%83%90%E3%82%A4%E3%83%88-ef014c979c2f50e7",
]

def test_url(page, url):
    print(f"\n{'='*60}")
    print(f"URL: {url[:80]}")
    print(f"{'='*60}")

    page.goto(url, wait_until="domcontentloaded", timeout=20000)
    page.wait_for_timeout(2500)

    sections = page.query_selector_all("[class*='JobDescriptionBlockSection']")

    # ① 全セクションのヘッダー一覧
    print("\n【① 全セクションのヘッダー一覧】")
    for si, section in enumerate(sections):
        header = section.query_selector("[class*='JobDescriptionBlockSection-headerText']")
        header_text = (header.inner_text() or "").strip() if header else "(ヘッダーなし)"
        print(f"  [{si}] {header_text!r}")

    # ② 「勤務地所在地」セクション
    print("\n【② 「勤務地所在地」セクション】")
    found = False
    for si, section in enumerate(sections):
        header = section.query_selector("[class*='JobDescriptionBlockSection-headerText']")
        if header and (header.inner_text() or "").strip() == "勤務地所在地":
            full = (section.inner_text() or "").strip()
            address = full.replace("勤務地所在地", "").strip()
            print(f"  セクション[{si}]: {address!r}")
            found = True
            break
    if not found:
        print("  見つからず")

    # ③ 「勤務地」セクション（フォールバック）
    print("\n【③ 「勤務地」セクション（フォールバック）】")
    found = False
    for si, section in enumerate(sections):
        header = section.query_selector("[class*='JobDescriptionBlockSection-headerText']")
        if header and (header.inner_text() or "").strip() == "勤務地":
            full = (section.inner_text() or "").strip()
            address = full.replace("勤務地", "").strip()
            print(f"  セクション[{si}]: {address!r}")
            found = True
            break
    if not found:
        print("  見つからず")

    # ④ 「アクセス」セクション：複数パターン正規表現でテスト
    _ST = r'[^\s・、,]+'
    STATION_PATTERNS = [
        (rf'「({_ST}駅)」[^\n]*?(?:直結|すぐ|スグ|徒歩(?:約)?(\d+)分)',   '「」ありパターン（直結・すぐ・スグ含む）'),
        (rf'({_ST}駅)[よかまでりら]{{1,4}}[^\n]*?徒歩(?:約)?(\d+)分',     'より/から/まで'),
        (rf'({_ST}駅)\s+徒歩(?:約)?(\d+)分',                              'スペース区切り'),
        (rf'({_ST}駅)[^\n]*?(?:直結|すぐ|スグ|徒歩(?:約)?(\d+)分)',        '汎用（出口名・直結・すぐ・スグ含む）'),
    ]
    print("\n【④ 「アクセス」セクション（複数パターン正規表現）】")
    found = False
    for si, section in enumerate(sections):
        header = section.query_selector("[class*='JobDescriptionBlockSection-headerText']")
        if header and (header.inner_text() or "").strip() == "アクセス":
            found = True
            candidates = []
            for li in section.query_selector_all("li"):
                li_text = (li.inner_text() or "").strip()
                print(f"  li: {li_text!r}")
                for pat, label in STATION_PATTERNS:
                    m = re.search(pat, li_text)
                    if m:
                        walk = int(m.group(2)) if m.group(2) else 0
                        candidates.append((walk, m.group(1)))
                        print(f"    → [{label}] 駅={m.group(1)}, 徒歩={m.group(2) or '直結(0)'}分")
                        break
            if candidates:
                candidates.sort(key=lambda x: x[0])
                raw = candidates[0][1]
                m_line = re.search(r'(?:線／?|／)(\S+駅)', raw)
                if m_line:
                    final = m_line.group(1)
                else:
                    cleaned = re.sub(r'^[A-Za-z]+', '', raw)
                    final = cleaned if cleaned.endswith('駅') else raw
                print(f"  ★ 選択: {final}（徒歩{candidates[0][0]}分）{'← 路線名除去: ' + raw if m_line else ''}")
            else:
                print("  ★ マッチなし（駅取得できず）")
            break
    if not found:
        print("  「アクセス」セクションが見つかりませんでした")

    print(f"\n【ページタイトル】{page.title()}")


def main():
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
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
        for url in URLS:
            test_url(page, url)
            page.wait_for_timeout(2000)
        browser.close()
    print("\n\n✅ テスト完了")


if __name__ == "__main__":
    main()
