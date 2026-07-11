import os
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import scratchattach as scratch3
from dotenv import load_dotenv
import sys

load_dotenv()

# ==========================================
# 🔒 セキュリティ・環境変数管理
# ==========================================
SESSION_ID = os.getenv("SCRATCH_SESSION_ID")
USERNAME = os.getenv("SCRATCH_USERNAME")
PROJECT_ID = os.getenv("SCRATCH_PROJECT_ID")

if not all([SESSION_ID, USERNAME, PROJECT_ID]):
    print("❌ エラー: .env ファイルに必要な設定が見つかりません。")
    exit(1)
else:
    PROJECT_ID = int(PROJECT_ID)

# クラウド変数名の定義
CLOUD_VAR_LINE = "JR東日本"        
CLOUD_VAR_FLAG = "現在送信中か"    
CLOUD_VAR_CONTINUE = "続きがあるか" 
CLOUD_VAR_UPDATE = "最終更新"      
CLOUD_VAR_BOOT = "起動"            

# ⏱️ 定期更新タイマー設定
last_success_time = time.time()        
INTERVAL = 600  # 10分（600秒）

# ==========================================
# 🗺️ スクレイピング＆データ送信コアロジック
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
    now = datetime.now()
    return now.strftime("%y%m%d%H%M%S")

def scrape_and_sync_push(connection):
    print("\n--- 🗺️ 運行情報をスクレイピング中 ---")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        items = soup.find_all('li', class_='traininfo-routes__table__item')
        
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
        print(f"✅ スクレイピング完了。全 {total_steps} 個のデータを送信します。")
        
        for index, (name, data, cont_flag, chunk_info) in enumerate(send_queue, start=1):
            print(f"  🔄 [{index}/{total_steps}] 送信中: {name} ({chunk_info})")
            
            connection.set_var(CLOUD_VAR_CONTINUE, cont_flag)
            connection.set_var(CLOUD_VAR_LINE, data)
            connection.set_var(CLOUD_VAR_FLAG, "1")
            
            # Scratch側のリセット（0に戻るの）を待つ
            start_wait = time.time()
            while True:
                try:
                    current_flag = scratch3.get_var(PROJECT_ID, CLOUD_VAR_FLAG)
                    if current_flag == "0" or current_flag == 0:
                        break
                except Exception:
                    pass 
                if time.time() - start_wait > 5.0:
                    print("  ⚠️ Scratch側リセットのタイムアウトを検知。")
                    break
                time.sleep(0.1) 
                
        time_string = get_formatted_now()
        print(f"🚀 全路線の送信完了。最終更新日時を送信: {time_string}")
        connection.set_var(CLOUD_VAR_UPDATE, time_string)
        print("✅ 「最終更新」の書き込みが完了しました。")
        return True  

    except Exception as e:
        print(f"❌ 同期中にエラーが発生しました: {e}")
        return False  

# ==========================================
# 🏎️ メイン監視ループ
# ==========================================
if __name__ == "__main__":
    print("🚀 Scratch運行情報同期システム（シンプル安全版）を起動しました。")
    print(f"📡 対象プロジェクト ID: {PROJECT_ID}")
    
    session = scratch3.Session(SESSION_ID, username=USERNAME)
    connection = session.connect_cloud(project_id=PROJECT_ID)
    
    print("\n🛡️ リアルタイム監視中... 終了するには Ctrl+C を押してください。")
    sys.stdout.flush()

    try:
        while True:
            current_time = time.time()
            
            # 🚩 1. 手動起動フラグ（☁ 起動）のチェック
            boot_flag = "0"
            try:
                boot_flag = str(scratch3.get_var(PROJECT_ID, CLOUD_VAR_BOOT))
            except Exception:
                pass
                
            if boot_flag == "1":
                print(f"\n🔔 手動起動シグナルを検知しました。処理を開始します。")
                
                # 起動フラグを即座に「0」に戻し、送信中であることを示す
                try:
                    connection.set_var(CLOUD_VAR_BOOT, "0")
                except Exception:
                    pass
                
                # 送信処理を実行（この処理が動いている間、whileループは止まるため連打は100%無視されます）
                success = scrape_and_sync_push(connection)
                
                if success:
                    last_success_time = time.time()  # 10分自動更新タイマーをリセット
                    print("⏱️ 送信成功に伴い、10分自動更新タイマーをリセットしました。")
                else:
                    print("⚠️ 送信失敗。")
                sys.stdout.flush()

            # ⏱️ 2. 定期自動実行（10分おき）のチェック
            elif current_time - last_success_time >= INTERVAL:
                print(f"\n⏱️ 定期更新の時刻になりました（10分自動実行）")
                success = scrape_and_sync_push(connection)
                if success:
                    last_success_time = current_time
                sys.stdout.flush()

            # 1秒間スリープ（ここがOSへの優しさとなり、Ctrl+C を確実に通します）
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Ctrl+C を検知しました。プログラムを安全に完全終了します。")
