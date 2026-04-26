# -*- coding: utf-8 -*-
"""
スクレイピングのデバッグ用テストスクリプト
指定URLのページを開いて、エリア・最寄り駅・アクセスセクションの中身を全部表示する
"""
import re

URLS = [
    "http://jp.indeed.com/job/%E9%9F%93%E5%9B%BD%E6%96%99%E7%90%86%E5%B0%82%E9%96%80%E5%BA%97%E3%81%AE%E3%83%A9%E3%83%B3%E3%83%81%E3%82%B9%E3%82%BF%E3%83%83%E3%83%95-197f6bbf7a05d7f5",
    "http://jp.indeed.com/job/%E5%8F%B0%E6%B9%BE%E3%83%90%E3%83%AB%E3%81%AE%E3%83%9B%E3%83%BC%E3%83%AB%E3%82%B9%E3%82%BF%E3%83%83%E3%83%95%E3%82%A2%E3%83%AB%E3%83%90%E3%82%A4%E3%83%88-0abc2f584d14b95d",
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

    # ④ 「アクセス」セクション：新正規表現でテスト
    print("\n【④ 「アクセス」セクション（新正規表現: 「駅名」.*?徒歩X分）】")
    found = False
    for si, section in enumerate(sections):
        header = section.query_selector("[class*='JobDescriptionBlockSection-headerText']")
        if header and (header.inner_text() or "").strip() == "アクセス":
            found = True
            candidates = []
            for li in section.query_selector_all("li"):
                li_text = (li.inner_text() or "").strip()
                m = re.search(r'「(\S+駅)」.*?徒歩(\d+)分', li_text)
                print(f"  li: {li_text!r}")
                if m:
                    candidates.append((int(m.group(2)), m.group(1)))
                    print(f"    → マッチ: 駅={m.group(1)}, 徒歩={m.group(2)}分")
            if candidates:
                candidates.sort(key=lambda x: x[0])
                print(f"  ★ 選択: {candidates[0][1]}（徒歩{candidates[0][0]}分）")
            break
    if not found:
        print("  見つからず")

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
