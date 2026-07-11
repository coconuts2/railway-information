"""
Scratch 運行情報同期システム（JR東日本 東北エリア → Scratch クラウド変数）

対象環境: Render Web Service / scratchattach 2.x

【この版で対処した既知の問題】
1. scratchattach v2 には CloudConnection / Cloud クラスが存在しない
   → sa.login_by_id() → session.connect_cloud() が正解
2. Render Web Service はポートを bind しないとデプロイが落ちる
   → ヘルスサーバをメインループより前に、別スレッドで起動する
3. Render 無料プランは「インバウンドHTTPが15分間無い」とスピンダウンする
   （CPUが動いていても関係なく落とされ、転送が途中でブツ切れになる）
   → 自己 ping スレッドを用意。ただし外部監視(UptimeRobot 等)の併用を強く推奨
4. ScratchCloud.get_var() は ☁ プレフィックスのキー不整合により
   リアルタイム更新を拾えない（ログAPIのスナップショットを返し続ける）
   → get_var は一切使わず、cloud.events() で自前に状態を保持する
5. cloud.reconnect() / disconnect() はイベントリスナーの websocket も閉じてしまう
   → 送信側の復旧には cloud.connect() のみを使う
"""

import os
import sys
import time
import threading
import warnings
import http.server
import urllib.request
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

import scratchattach as sa

warnings.filterwarnings("ignore", category=sa.LoginDataWarning)
load_dotenv()


def log(msg):
    print(msg, flush=True)


# ==========================================================
# 🔒 設定
# ==========================================================
SESSION_ID = os.environ.get("SCRATCH_SESSION_ID")
USERNAME = os.environ.get("SCRATCH_USERNAME")
PROJECT_ID = os.environ.get("SCRATCH_PROJECT_ID")

# クラウド変数名（☁ 記号・先頭スペースは含めない）
CLOUD_VAR_LINE = "JR東日本"
CLOUD_VAR_FLAG = "現在送信中か"
CLOUD_VAR_CONTINUE = "続きがあるか"
CLOUD_VAR_UPDATE = "最終更新"
CLOUD_VAR_BOOT = "起動"

URL = "https://traininfo.jreast.co.jp/train_info/tohoku.aspx"
CHUNK_SIZE = 256              # Scratch のクラウド変数長上限
INTERVAL = 600                # 定期更新の間隔（秒）
ACK_TIMEOUT = 5.0             # Scratch が「現在送信中か」を 0 に戻すのを待つ上限
MAX_CONSECUTIVE_TIMEOUTS = 3  # 連続タイムアウトでその回の送信を打ち切る

DEBUG_EVENTS = os.environ.get("DEBUG_CLOUD_EVENTS") == "1"

if not all([SESSION_ID, USERNAME, PROJECT_ID]):
    log("❌ 起動中止: SCRATCH_SESSION_ID / SCRATCH_USERNAME / SCRATCH_PROJECT_ID を設定してください。")
    sys.exit(1)

PROJECT_ID = str(int(PROJECT_ID))  # 数値であることの検証も兼ねる


# ==========================================================
# 🌐 Render 用ヘルスサーバ（必ずメインループより前に、別スレッドで）
# ==========================================================
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK: scratch sync running")

    def log_message(self, *args):
        pass  # アクセスログを抑制


def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    http.server.HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()


# ==========================================================
# ⏰ スピンダウン対策の自己 ping
#    Render は RENDER_EXTERNAL_URL を自動で入れてくれる。
#    ※ 一度眠ってしまうと自己 ping では起こせない。
#      UptimeRobot / cron-job.org からの外部監視を必ず併用すること。
# ==========================================================
def start_keepalive():
    external_url = os.environ.get("RENDER_EXTERNAL_URL")
    if not external_url:
        log("ℹ️ RENDER_EXTERNAL_URL が無いため自己 ping は無効です。")
        return

    def loop():
        while True:
            time.sleep(600)  # 10分おき（スピンダウンは15分無通信で発動）
            try:
                urllib.request.urlopen(external_url, timeout=20).read()
            except Exception as e:
                log(f"⚠️ keepalive ping 失敗: {e}")

    threading.Thread(target=loop, daemon=True).start()
    log(f"⏰ 自己 ping を有効化しました（{external_url}）")


# ==========================================================
# ☁ クラウド変数の状態を「自前で」保持する
#    ★ cloud.get_var() は使わない（キー不整合でライブ更新を拾えないため）
# ==========================================================
cloud_state = {}
state_lock = threading.Lock()
flag_reset = threading.Event()  # 「現在送信中か」が 0 に戻ったら set される


def get_cached(name, default=None):
    with state_lock:
        return cloud_state.get(name, default)


def set_cached(name, value):
    with state_lock:
        cloud_state[name] = str(value)


def setup_cloud():
    session = sa.login_by_id(SESSION_ID, username=USERNAME)
    cloud = session.connect_cloud(PROJECT_ID)
    log("🟢 Scratch クラウド変数サーバーへの接続に成功しました。")

    # --- 初期値をクラウドログから拾う（use_logs=True は recorder を作らないので安全）---
    try:
        seed = cloud.get_all_vars(use_logs=True)  # キーは "☁ 起動" 形式で返る
        if seed:
            with state_lock:
                for k, v in seed.items():
                    cloud_state[k.removeprefix("☁ ")] = str(v)
            log(f"📋 クラウドログから初期値を取得: {sorted(cloud_state)}")
        else:
            log("⚠️ クラウドログが空です（直近100件に該当なし）。動作は継続します。")
    except Exception as e:
        log(f"⚠️ クラウドログの取得に失敗: {e}")

    # --- websocket でリアルタイム監視 ---
    events = cloud.events()

    @events.event
    def on_ready():
        log("👂 クラウド変数のリアルタイム監視を開始しました。")

    @events.event
    def on_set(activity):
        name = str(activity.var)   # ☁ が剥がされた名前で届く
        value = str(activity.value)
        if DEBUG_EVENTS:
            log(f"   [EVENT] {name!r} = {value!r}")
        set_cached(name, value)
        if name == CLOUD_VAR_FLAG and value == "0":
            flag_reset.set()

    @events.event
    def on_reconnect():
        log("🔁 監視用 websocket が再接続しました。")

    events.start(thread=True)
    return cloud, events


def safe_set_vars(cloud, var_value_dict, retries=2):
    """
    複数変数を1つの websocket フレームで原子的に送る。
    復旧には connect() を使う（reconnect()/disconnect() はリスナーを殺すので禁止）。
    """
    for attempt in range(retries + 1):
        try:
            cloud.set_vars(var_value_dict)  # dict の順序どおりに送信される
            return True
        except Exception as e:
            log(f"  ⚠️ set_vars 失敗: {e}")
            if attempt < retries:
                try:
                    cloud.connect()
                    log("  🔁 送信用 websocket を再接続しました。")
                except Exception as e2:
                    log(f"  ❌ 再接続失敗: {e2}")
                time.sleep(1)
    return False


def safe_set_var(cloud, name, value, retries=2):
    return safe_set_vars(cloud, {name: value}, retries=retries)


# ==========================================================
# 🔤 エンコード
# ==========================================================
CHAR_TO_CODE = {str(i): str(i).zfill(2) for i in range(10)}
CHAR_TO_CODE.update({chr(ord("a") + i): str(10 + i) for i in range(26)})


def get_status_code(status_text):
    if "運転を見合わせています" in status_text or "運休" in status_text or "見合わせ" in status_text:
        return 3
    if "遅れ" in status_text or "遅延" in status_text:
        return 2
    if "平常運転" in status_text or "平常どおり" in status_text:
        return 1
    if "お知らせ" in status_text or "見込まれる" in status_text or "可能性" in status_text:
        return 4
    return 0


def encode_detail_text_complex(text):
    if not text:
        return ""
    out = []
    for char in text:
        for hex_digit in hex(ord(char))[2:].zfill(4):
            d = hex_digit.lower()
            if d in CHAR_TO_CODE:
                out.append(CHAR_TO_CODE[d])
    return "".join(out)


def get_formatted_now():
    return datetime.now().strftime("%y%m%d%H%M%S")


# ==========================================================
# 🗺️ スクレイピング
# ==========================================================
def scrape_lines():
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

    grouped = {}
    for item in soup.find_all("li", class_="traininfo-routes__table__item"):
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

        if rosen_name not in grouped:
            grouped[rosen_name] = {
                "max_status_code": status_code,
                "combined_text": detail_text,
            }
        else:
            g = grouped[rosen_name]
            if detail_text:
                g["combined_text"] = (
                    g["combined_text"] + " " + detail_text if g["combined_text"] else detail_text
                )
            g["max_status_code"] = max(g["max_status_code"], status_code)

    return grouped


def build_send_queue(grouped):
    queue = []
    for rosen_name, info in grouped.items():
        full_data = f"{info['max_status_code']}{encode_detail_text_complex(info['combined_text'])}"
        chunks = [full_data[i : i + CHUNK_SIZE] for i in range(0, len(full_data), CHUNK_SIZE)]
        n = len(chunks)
        for idx, chunk in enumerate(chunks):
            if n == 1:
                cont = "0"    # 単独
            elif idx == 0:
                cont = "1"    # 先頭
            elif idx == n - 1:
                cont = "3"    # 末尾
            else:
                cont = "2"    # 中間
            queue.append((rosen_name, chunk, cont, f"{idx + 1}/{n}"))
    return queue


# ==========================================================
# 🚀 送信
# ==========================================================
def scrape_and_sync_push(cloud):
    log("\n--- 🗺️ 運行情報をスクレイピング中 ---")
    try:
        send_queue = build_send_queue(scrape_lines())
    except Exception as e:
        log(f"❌ スクレイピングに失敗しました: {e}")
        return False

    total = len(send_queue)
    if total == 0:
        log("⚠️ 送信すべきデータがありません（HTML構造が変わった可能性）。")
        return False

    log(f"✅ スクレイピング完了。全 {total} 個のチャンクを送信します。")
    started = time.time()
    consecutive_timeouts = 0

    for index, (name, data, cont_flag, chunk_info) in enumerate(send_queue, start=1):
        log(f"  🔄 [{index}/{total}] {name} ({chunk_info})")

        # ★ 先にクリアしてから、3変数を1フレームで送信（フラグは必ず最後）
        flag_reset.clear()
        ok = safe_set_vars(
            cloud,
            {
                CLOUD_VAR_CONTINUE: cont_flag,
                CLOUD_VAR_LINE: data,
                CLOUD_VAR_FLAG: "1",
            },
        )
        if not ok:
            log("❌ 送信不能。この回の同期を中止します。")
            return False
        set_cached(CLOUD_VAR_FLAG, "1")

        # ポーリングせずイベントで待つ（Scratch が 0 に戻した瞬間に即復帰）
        if flag_reset.wait(timeout=ACK_TIMEOUT):
            consecutive_timeouts = 0
        else:
            consecutive_timeouts += 1
            log(f"  ⚠️ Scratch からの応答なし（{consecutive_timeouts} 回連続）")
            if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS:
                log("🛑 Scratch プロジェクトが動作していないようです。この回の同期を中止します。")
                log("   （このシステムはプロジェクトを開いている間だけデータを受信できます）")
                return False

    time_string = get_formatted_now()
    safe_set_var(cloud, CLOUD_VAR_UPDATE, time_string)
    elapsed = time.time() - started
    log(f"🚀 全 {total} チャンク送信完了（{elapsed:.1f}秒）。最終更新: {time_string}")
    return True


# ==========================================================
# 🏎️ メイン
# ==========================================================
def main():
    threading.Thread(target=start_health_server, daemon=True).start()
    log(f"🌐 ヘルスサーバ起動 :{os.environ.get('PORT', 10000)}")
    start_keepalive()

    log(f"🚀 同期システム起動。対象プロジェクト: {PROJECT_ID}")

    try:
        cloud, _events = setup_cloud()
    except Exception as e:
        log(f"❌ クラウド接続の初期化に失敗しました: {e}")
        sys.exit(1)

    last_attempt = time.time()
    log("\n🛡️ 監視中...")

    while True:
        try:
            boot = str(get_cached(CLOUD_VAR_BOOT, "0"))

            if boot == "1":
                log("\n🔔 手動起動シグナル（☁ 起動 = 1）を検知しました。")
                safe_set_var(cloud, CLOUD_VAR_BOOT, "0")
                set_cached(CLOUD_VAR_BOOT, "0")  # 多重発火を防ぐ
                last_attempt = time.time()
                scrape_and_sync_push(cloud)

            elif time.time() - last_attempt >= INTERVAL:
                log("\n⏱️ 定期更新の時刻になりました。")
                last_attempt = time.time()  # 成否に関わらず更新（失敗時の連打を防ぐ）
                scrape_and_sync_push(cloud)

        except Exception as e:
            log(f"❌ メインループで予期しないエラー: {e}")
            time.sleep(5)

        time.sleep(0.5)


if __name__ == "__main__":
    main()
