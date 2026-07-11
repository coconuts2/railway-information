import os
import sys
import time
import threading
import http.server
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

import scratchattach as sa  # ★ v2 系。scratch3 という別名は付けない（v1 の名残で混乱するため）

load_dotenv()

# ==========================================
# 🌐 Render(Web Service)用ダミーサーバ
#    ★ 必ず「メインループより前」に「別スレッド」で起動すること。
#      さもないと "Port scan timeout reached, no open ports detected" でデプロイ失敗する。
# ==========================================
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK: scratch sync running")

    def log_message(self, *args):  # アクセスログを黙らせる
        pass


def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    http.server.HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()


threading.Thread(target=start_health_server, daemon=True).start()
print(f"🌐 ヘルスチェック用サーバを :{os.environ.get('PORT', 10000)} で起動しました。")

# ==========================================
# 🔒 環境変数
# ==========================================
SESSION_ID = os.environ.get("SCRATCH_SESSION_ID")
USERNAME = os.environ.get("SCRATCH_USERNAME")
PROJECT_ID = os.environ.get("SCRATCH_PROJECT_ID")

print("--- 🔍 環境変数 読み込み診断 ---")
print(f"SCRATCH_USERNAME: {USERNAME}")
print(f"SCRATCH_PROJECT_ID: {PROJECT_ID}")
if SESSION_ID:
    print(f"SCRATCH_SESSION_ID: OK（{len(SESSION_ID)}文字 / 先頭: {SESSION_ID[:2]}...）")
else:
    print("❌ SCRATCH_SESSION_ID: 読み込めませんでした（空っぽです）")
print("--------------------------------")

if not all([SESSION_ID, USERNAME, PROJECT_ID]):
    print("❌ システム停止: 必要な環境変数が揃っていません。")
    sys.exit(1)

PROJECT_ID = str(int(PROJECT_ID))  # 数値であることの検証を兼ねて str 化

# クラウド変数名（☁ 記号も先頭スペースも含めない）
CLOUD_VAR_LINE = "JR東日本"
CLOUD_VAR_FLAG = "現在送信中か"
CLOUD_VAR_CONTINUE = "続きがあるか"
CLOUD_VAR_UPDATE = "最終更新"
CLOUD_VAR_BOOT = "起動"

INTERVAL = 600  # 10分
last_success_time = time.time()

URL = "https://traininfo.jreast.co.jp/train_info/tohoku.aspx"


# ==========================================
# ☁ クラウド接続（scratchattach v2 の正しい書き方）
# ==========================================
def connect_cloud():
    """
    v2 では以下が正解。
      - sa.CloudConnection / sa.Cloud は存在しない（v1 の名前）
      - sa.Session(...) を直接生成しない（id が None になり TypeError の原因）
      - sa.login_by_id() → session.connect_cloud() の2段構え
    """
    session = sa.login_by_id(SESSION_ID, username=USERNAME)
    return session.connect_cloud(PROJECT_ID)  # -> sa.ScratchCloud


def safe_set_var(cloud, name, value, retries=2):
    """websocket が切れていたら再接続してリトライする"""
    for attempt in range(retries + 1):
        try:
            cloud.set_var(name, value)
            return True
        except Exception as e:
            print(f"  ⚠️ set_var 失敗 ({name}): {e}")
            if attempt < retries:
                try:
                    cloud.reconnect()
                    print("  🔁 クラウドへ再接続しました。")
                except Exception as e2:
                    print(f"  ❌ 再接続失敗: {e2}")
                time.sleep(1)
    return False


def safe_get_var(cloud, name):
    """
    v2 の get_var は初回呼び出しで CloudRecorder（websocket 監視）が起動し、
    以降は記録済みの値を返すのでループ内で叩いても API を叩き潰さない。
    """
    try:
        return cloud.get_var(name)
    except Exception:
        return None


# ==========================================
# 🔤 エンコード関連（元コードのまま）
# ==========================================
def get_status_code(status_text):
    if "運転を見合わせています" in status_text or "運休" in status_text or "見合わせ" in status_text:
        return 3
    elif "遅れ" in status_text or "遅延" in status_text:
        return 2
    elif "平常運転" in status_text or "平常どおり" in status_text:
        return 1
    elif "お知らせ" in status_text or "見込まれる" in status_text or "可能性" in status_text:
        return 4
    else:
        return 0


CHAR_TO_CODE = {}
for i in range(10):
    CHAR_TO_CODE[str(i)] = str(i).zfill(2)
for i in range(26):
    CHAR_TO_CODE[chr(ord("a") + i)] = str(10 + i)


def encode_detail_text_complex(text):
    if not text:
        return ""
    encoded_list = []
    for char in text:
        hex_code = hex(ord(char))[2:].zfill(4)
        for hex_digit in hex_code:
            d = hex_digit.lower()
            if d in CHAR_TO_CODE:
                encoded_list.append(CHAR_TO_CODE[d])
    return "".join(encoded_list)


def get_formatted_now():
    return datetime.now().strftime("%y%m%d%H%M%S")


# ==========================================
# 🗺️ スクレイピング＆送信
# ==========================================
def scrape_and_sync_push(cloud):
    print("\n--- 🗺️ 運行情報をスクレイピング中 ---")
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
        response = requests.get(URL, headers=headers, timeout=15)
        response.raise_for_status()
        response.encoding = "utf-8"
        soup = BeautifulSoup(response.text, "html.parser")

        items = soup.find_all("li", class_="traininfo-routes__table__item")

        grouped_lines = {}
        for item in items:
            name_tag = item.find("span", class_="traininfo-routes__name")
            if not name_tag:
                continue
            rosen_name = name_tag.text.strip()

            status_tag = item.find("p", class_="traininfo-routes__status")
            status_text = (
                status_tag.find("span").text.strip()
                if (status_tag and status_tag.find("span"))
                else "不明"
            )

            detail_tag = item.find("p", class_="traininfo-routes__note")
            detail_text = detail_tag.text.strip().replace("\n", " ") if detail_tag else ""

            status_code = get_status_code(status_text)

            if rosen_name not in grouped_lines:
                grouped_lines[rosen_name] = {
                    "max_status_code": status_code,
                    "combined_text": detail_text,
                }
            else:
                if detail_text:
                    if grouped_lines[rosen_name]["combined_text"]:
                        grouped_lines[rosen_name]["combined_text"] += " " + detail_text
                    else:
                        grouped_lines[rosen_name]["combined_text"] = detail_text
                if status_code > grouped_lines[rosen_name]["max_status_code"]:
                    grouped_lines[rosen_name]["max_status_code"] = status_code

        send_queue = []
        for rosen_name, info in grouped_lines.items():
            status_code = info["max_status_code"]
            encoded_detail = encode_detail_text_complex(info["combined_text"])

            full_data = f"{status_code}{encoded_detail}"
            chunks = [full_data[i : i + 256] for i in range(0, len(full_data), 256)]
            num_chunks = len(chunks)

            for c_idx, chunk_data in enumerate(chunks):
                if num_chunks == 1:
                    continue_flag = "0"
                elif c_idx == 0:
                    continue_flag = "1"
                elif c_idx == num_chunks - 1:
                    continue_flag = "3"
                else:
                    continue_flag = "2"

                send_queue.append(
                    (rosen_name, chunk_data, continue_flag, f"{c_idx + 1}/{num_chunks}")
                )

        total_steps = len(send_queue)
        print(f"✅ スクレイピング完了。全 {total_steps} 個のデータを送信します。")

        for index, (name, data, cont_flag, chunk_info) in enumerate(send_queue, start=1):
            print(f"  🔄 [{index}/{total_steps}] 送信中: {name} ({chunk_info})")

            safe_set_var(cloud, CLOUD_VAR_CONTINUE, cont_flag)
            safe_set_var(cloud, CLOUD_VAR_LINE, data)
            safe_set_var(cloud, CLOUD_VAR_FLAG, "1")

            # Scratch 側が 0 に戻すのを待つ
            start_wait = time.time()
            while True:
                if str(safe_get_var(cloud, CLOUD_VAR_FLAG)) == "0":
                    break
                if time.time() - start_wait > 5.0:
                    print("  ⚠️ Scratch側リセットのタイムアウトを検知。")
                    break
                time.sleep(0.1)

        time_string = get_formatted_now()
        print(f"🚀 全路線の送信完了。最終更新日時を送信: {time_string}")
        safe_set_var(cloud, CLOUD_VAR_UPDATE, time_string)
        print("✅ 「最終更新」の書き込みが完了しました。")
        return True

    except Exception as e:
        print(f"❌ 同期中にエラーが発生しました: {e}")
        return False


# ==========================================
# 🏎️ メイン監視ループ
# ==========================================
if __name__ == "__main__":
    print("🚀 Scratch運行情報同期システムを起動しました。")
    print(f"📡 対象プロジェクト ID: {PROJECT_ID}")

    try:
        cloud = connect_cloud()
        print("🟢 Scratch クラウド変数サーバーへの接続に成功しました。")
    except Exception as e:
        print(f"❌ クラウド接続の初期化に失敗しました: {e}")
        sys.exit(1)

    # 初回 get_var で CloudRecorder（websocket 監視）を起動しておく
    print(f"（初期値）{CLOUD_VAR_BOOT} = {safe_get_var(cloud, CLOUD_VAR_BOOT)}")

    print("\n🛡️ リアルタイム監視中...")
    sys.stdout.flush()

    while True:
        current_time = time.time()

        # 🚩 手動起動フラグ
        boot_flag = str(safe_get_var(cloud, CLOUD_VAR_BOOT))

        if boot_flag == "1":
            print("\n🔔 手動起動シグナルを検知しました。処理を開始します。")
            safe_set_var(cloud, CLOUD_VAR_BOOT, "0")

            if scrape_and_sync_push(cloud):
                last_success_time = time.time()
                print("⏱️ 送信成功に伴い、10分自動更新タイマーをリセットしました。")
            else:
                print("⚠️ 送信失敗。")
            sys.stdout.flush()

        # ⏱️ 定期自動実行
        elif current_time - last_success_time >= INTERVAL:
            print("\n⏱️ 定期更新の時刻になりました（10分自動実行）")
            if scrape_and_sync_push(cloud):
                last_success_time = current_time
            sys.stdout.flush()

        time.sleep(1)
