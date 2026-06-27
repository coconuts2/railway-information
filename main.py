import os
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import scratchattach as scratch3
import threading                                            # 👈 追加しました
from http.server import HTTPServer, BaseHTTPRequestHandler  # 👈 追加しました

# === RenderのPORT（窓口）を維持するためのダミーWEBサーバー ===
class DummyServerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write("運行情報常駐監視システム稼働中".encode("utf-8"))

    def log_message(self, format, *args):
        pass # Renderのログがアクセス通知で埋まるのを防ぐため、ログを非表示にします

def run_dummy_server(port):
    server_address = ("", port)
    httpd = HTTPServer(server_address, DummyServerHandler)
    httpd.serve_forever()
# ==========================================================

load_dotenv()

# ==========================================
# 🔒 セキュリティ管理
# ==========================================
SESSION_ID = os.getenv("SCRATCH_SESSION_ID")
USERNAME = os.getenv("SCRATCH_USERNAME")
PROJECT_ID = os.getenv("SCRATCH_PROJECT_ID")

if not all([SESSION_ID, USERNAME, PROJECT_ID]):
    print("❌ エラー: .env ファイルに必要な設定が見つかりません。")
    exit(1)
else:
    PROJECT_ID = int(PROJECT_ID)

# --- Scratch側のクラウド変数名（「☁ 」は自動補完されます） ---
CLOUD_VAR_LINE = "JR東日本"        # 【分割された1データ】を送信する変数
CLOUD_VAR_FLAG = "現在送信中か"    # 同期用フラグ（1: 送信中 / 0: 完了待機）
CLOUD_VAR_CONTINUE = "続きがあるか" # 分割状態フラグ（0: 一発終了 / 1: 最初 / 2: 途中 / 3: 最後）
CLOUD_VAR_UPDATE = "最終更新"      # 📅 12桁固定（YYMMDDHHMMSS）の日時を入れる変数
CLOUD_VAR_BOOT = "起動"            # 🚩 Scratch側で旗が押されたことを検知する変数
# ==========================================

url = 'https://traininfo.jreast.co.jp/train_info/tohoku.aspx'

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

# --- 詳細エンコードの定義: 0-9, a-z を 00-35 にマッピング ---
CHAR_TO_CODE = {}
for i in range(10):
    CHAR_TO_CODE[str(i)] = str(i).zfill(2)
for i in range(26):
    char = chr(ord('a') + i)
    CHAR_TO_CODE[char] = str(10 + i)

def encode_detail_text_complex(text):
    if not text:
        return ""
    encoded_list = []
    for char in text:
        hex_code = hex(ord(char))[2:].zfill(4) 
        for hex_digit in hex_code:
            digit_lower = hex_digit.lower()
            if digit_lower in CHAR_TO_CODE:
                encoded_list.append(CHAR_TO_CODE[digit_lower])
    return "".join(encoded_list)

def get_formatted_now():
    """
    📅 現在日時を「YYMMDDHHMMSS」の12桁（0埋め）文字列にして返す関数
    例: 2026年3月5日 4時5分6秒 -> "260305040506"
    """
    now = datetime.now()
    # %y: 西暦の下2桁, %m: 月(01-12), %d: 日(01-31), %H: 時(00-23), %M: 分(00-59), %S: 秒(00-59)
    return now.strftime("%y%m%d%H%M%S")

def scrape_and_sync_push(connection):
    print("\n--- 🗺️ 運行情報をスクレイピング中 ---")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        items = soup.find_all('li', class_='traininfo-routes__table__item')
        
        # 同名路線の集約
        grouped_lines = {}
        for item in items:
            name_tag = item.find('span', class_='traininfo-routes__name')
            if not name_tag:
                continue
            rosen_name = name_tag.text.strip()
            
            status_tag = item.find('p', class_='traininfo-routes__status')
            status_text = status_tag.find('span').text.strip() if (status_tag and status_tag.find('span')) else "不明"
            
            detail_tag = item.find('p', class_='traininfo-routes__note')
            detail_text = detail_tag.text.strip().replace('\n', ' ') if detail_tag else ""
            
            status_code = get_status_code(status_text)
            
            if rosen_name not in grouped_lines:
                grouped_lines[rosen_name] = {
                    "max_status_code": status_code,
                    "combined_text": detail_text
                }
            else:
                if detail_text:
                    if grouped_lines[rosen_name]["combined_text"]:
                        grouped_lines[rosen_name]["combined_text"] += " " + detail_text
                    else:
                        grouped_lines[rosen_name]["combined_text"] = detail_text
                if status_code > grouped_lines[rosen_name]["max_status_code"]:
                    grouped_lines[rosen_name]["max_status_code"] = status_code

        # 送信キューの生成（256文字分割処理）
        send_queue = []
        for rosen_name, info in grouped_lines.items():
            status_code = info["max_status_code"]
            encoded_detail = encode_detail_text_complex(info["combined_text"])
            
            full_data = f"{status_code}{encoded_detail}"
            chunks = [full_data[i:i+256] for i in range(0, len(full_data), 256)]
            num_chunks = len(chunks)
            
            for c_idx, chunk_data in enumerate(chunks):
                if num_chunks == 1:
                    continue_flag = "0"
                else:
                    if c_idx == 0:
                        continue_flag = "1"
                    elif c_idx == num_chunks - 1:
                        continue_flag = "3"
                    else:
                        continue_flag = "2"
                
                send_queue.append((rosen_name, chunk_data, continue_flag, f"{c_idx+1}/{num_chunks}"))
            
        total_steps = len(send_queue)
        print(f"✅ スクレイピング完了。全 {total_steps} 個のデータを順番に送信します。")
        
        # --- ループ処理（分割ステップ数分繰り返す） ---
        for index, (name, data, cont_flag, chunk_info) in enumerate(send_queue, start=1):
            print(f"  🔄 [{index}/{total_steps}] 送信中: {name} ({chunk_info})")
            
            connection.set_var(CLOUD_VAR_CONTINUE, cont_flag)
            connection.set_var(CLOUD_VAR_LINE, data)
            connection.set_var(CLOUD_VAR_FLAG, "1")
            
            while True:
                try:
                    current_flag = scratch3.get_var(PROJECT_ID, CLOUD_VAR_FLAG)
                    if current_flag == "0" or current_flag == 0:
                        break
                except Exception:
                    pass 
                time.sleep(0.1) 
                
        # --- 📅 全区間送り終わったあとの「最終更新日時」送信処理 ---
        time_string = get_formatted_now()
        print(f"🚀 全路線の送信完了。最終更新日時を送信します: {time_string}")
        connection.set_var(CLOUD_VAR_UPDATE, time_string)
        print("✅ 「最終更新」の書き込みが完了しました。")

    except Exception as e:
        print(f"❌ 同期中にエラーが発生しました: {e}")

if __name__ == "__main__":
    print("🚀 Scratch運行情報管理システムを起動しました。")
    print(f"📡 ターゲットユーザー: coconuts2 / プロジェクトID: 1255095158")
    
    # 先にRender用のポート窓口を開放する（最重要）
    port = int(os.environ.get("PORT", 10000))
    web_thread = threading.Thread(target=run_dummy_server, args=(port,), daemon=True)
    web_thread.start()
    print(f"📡 Render用ダミーWeb待ち受けを開始しました (Port: {port})")

    # Scratch接続と常駐メインループ（PCで成功していた元の記述）
    session = scratch3.Session(SESSION_ID, username=USERNAME)
    connection = session.connect_cloud(project_id=PROJECT_ID)
       
    # 定期実行の間隔（10分 = 600秒）
    INTERVAL = 600
    last_run_time = 0

    print("\n💡 常駐監視モードで待機中... (終了するには Ctrl+C)")
    
    try:
        while True:
            current_time = time.time()
            
            # 🚩 条件A: Scratch側で緑の旗がクリックされ、「起動」が1になったかを検知
            boot_flag = "0"
            try:
                boot_flag = str(scratch3.get_var(PROJECT_ID, CLOUD_VAR_BOOT))
            except Exception:
                pass  # 通信一時エラーは無視して次のサイクルへ
                
            if boot_flag == "1":
                print("\n🚩 Scratch側での『起動(緑の旗)』を検知しました！初期化処理を行います。")
                try:
                    # 即座にPython側から「起動」を「0」に戻す
                    connection.set_var(CLOUD_VAR_BOOT, "0")
                    print("  -> クラウド変数『起動』を 0 にリセットしました。")
                except Exception as e:
                    print(f"  ❌ 『起動』変数のリセットに失敗: {e}")
                
                # 即時同期を実行
                scrape_and_sync_push(connection)
                last_run_time = time.time() # 定期実行のタイマーをリセット
                
            # ⏱️ 条件B: 前回の実行から10分が経過したかを検知（定期自動実行）
            elif current_time - last_run_time >= INTERVAL:
                print(f"\n⏱️ 定期更新の時刻になりました（10分おき自動実行）")
                scrape_and_sync_push(connection)
                last_run_time = current_time
            
            # サーバーやPCへの負荷を抑えるため、1秒ごとに各種フラグの状態をチェック
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nプログラムを安全に終了しました。")
