#!/usr/bin/env python3
"""trenches_bot.py v3 - screener token baru + narasi + bot Telegram multi-user. Hanya stdlib, Python 3.8+.

Data gratis tanpa API key: GeckoTerminal + DexScreener. TIDAK ada wallet/transaksi asli.
Narasi = tebakan dari nama/deskripsi token (BUKAN data viral X; X tidak punya API baca gratis).

Env:
  TELEGRAM_BOT_TOKEN   token bot (wajib untuk Telegram)
  TELEGRAM_CHAT_ID     chat ID pemilik/admin (wajib untuk Telegram)
  BOT_INVITE_CODE      opsional: /join HANDUR
  BOT_MAX_USERS        opsional: batas pelanggan (default 200)
  X_BEARER_TOKEN       opsional & BERBAYAR: hitung jumlah post X per token
  X_MAX_CALLS_PER_DAY  opsional: batas panggilan X per hari (default 20)
Pakai: python3 trenches_bot.py [--once] [--report] [--chain solana]
"""
import argparse, json, os, re, threading, time, urllib.parse, urllib.request, urllib.error
from pathlib import Path

DS = "https://api.dexscreener.com"
GT = "https://api.geckoterminal.com/api/v2"
UA = {"User-Agent": "trenches-bot/3.0", "Accept": "application/json"}
DATA = Path(os.environ.get("BOT_DATA", "./botdata"))
DATA.mkdir(exist_ok=True)
SEEN, PAPER, LOG = DATA / "seen.json", DATA / "paper.json", DATA / "signals.jsonl"
SUBS, NARR = DATA / "subscribers.json", DATA / "narratives.json"
LOCK = threading.Lock()

CFG = dict(
    chain="solana", poll=45,
    min_liq=8000, min_age_min=5, max_age_h=6,
    min_vol_h1=5000, min_txns_h1=40, min_buy_ratio=0.55,
    max_fdv=3_000_000,
    tp=1.0, sl=-0.4, max_hold_h=24, max_open=10,
)
GT_NET = {"solana": "solana", "bsc": "bsc", "base": "base", "ethereum": "eth"}
_last = [0.0]
STATE = {"last": "belum ada putaran", "cand": 0, "found": 0, "passed": 0}
DESC = {}           # alamat token -> deskripsi dari profil DexScreener
LASTCMD = {}        # chat -> waktu perintah terakhir (anti-spam)
XCALLS = {"day": "", "n": 0}

DISCLAIMER = ("PERINGATAN: ini screener momentum otomatis, BUKAN saran finansial. Token baru sangat "
              "berisiko (rug/honeypot). Cek keamanan sendiri, jangan pakai uang yang tak siap hilang.")

THEMES = {
    "AI / agent": ["ai", "agent", "agents", "gpt", "llm", "neural", "robot", "grok", "openai", "deepseek", "claude"],
    "anjing": ["dog", "doge", "inu", "shib", "puppy", "pup", "wof"],
    "kucing": ["cat", "kitty", "kitten", "meow", "neko", "popcat"],
    "katak / meme klasik": ["pepe", "frog", "wojak", "chad", "npc", "gigachad"],
    "politik / tokoh": ["trump", "maga", "biden", "elon", "musk", "vote", "president", "election", "kamala", "putin"],
    "hewan lain": ["monkey", "ape", "bear", "bull", "whale", "penguin", "hamster", "duck", "goat", "turtle", "bird"],
    "makanan": ["pizza", "burger", "taco", "cheese", "sushi", "coffee", "banana"],
    "budaya / hiburan": ["anime", "waifu", "gaming", "game", "music", "vibe", "meme"],
    "Indonesia": ["indo", "garuda", "nusantara", "rupiah", "jakarta", "bali"],
}


# ---------- HTTP data publik ----------
def get(url, gap=1.2):
    wait = gap - (time.time() - _last[0])
    if wait > 0:
        time.sleep(wait)
    _last[0] = time.time()
    if url.startswith("/"):
        url = DS + url
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=15) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print("429 rate limit, tidur 60 detik")
            time.sleep(60)
        else:
            print("HTTP", e.code, url[:80])
    except Exception as e:
        print("error jaringan:", e)
    return None


def load(path, default):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


# ---------- narasi ----------
def themes_of(text):
    low = text.lower()
    words = set(re.findall(r"[a-z0-9]+", low))
    return [n for n, kws in THEMES.items() if any((k in words) or (len(k) >= 5 and k in low) for k in kws)]


def record_narr(found):
    now = time.time()
    with LOCK:
        db = load(NARR, {})
        for a, p in found.items():
            vol = (p.get("volume") or {}).get("h1") or 0
            if vol < 1000:
                continue
            bt = p.get("baseToken") or {}
            th = themes_of("%s %s %s" % (bt.get("name", ""), bt.get("symbol", ""), DESC.get(a, "")))
            db[a] = dict(t=now, sym=bt.get("symbol", "?"), vol=vol, th=th)
        db = {a: v for a, v in db.items() if now - v["t"] < 6 * 3600}
        NARR.write_text(json.dumps(db))


def narasi_text():
    now, agg = time.time(), {}
    for v in load(NARR, {}).values():
        if now - v["t"] > 3 * 3600:
            continue
        for th in v["th"]:
            g = agg.setdefault(th, dict(n=0, vol=0, top=("", 0)))
            g["n"] += 1
            g["vol"] += v["vol"]
            if v["vol"] > g["top"][1]:
                g["top"] = (v["sym"], v["vol"])
    if not agg:
        return "Belum ada narasi terdeteksi (3 jam terakhir)."
    rows = sorted(agg.items(), key=lambda kv: -kv[1]["vol"])[:6]
    out = ["Narasi paling ramai (3 jam, dari nama/deskripsi token baru):"]
    for th, g in rows:
        out.append("- %s: %d token, vol 1j total $%s, teratas %s" % (th, g["n"], format(int(g["vol"]), ","), g["top"][0]))
    out.append("\nIni tebakan dari nama/deskripsi token, bukan data viral X. Cek X manual.")
    return "\n".join(out)


def x_links(p):
    sym = (p.get("baseToken") or {}).get("symbol", "")
    q = "https://x.com/search?q=%s&f=live" % urllib.parse.quote("$" + sym)
    proj = ""
    for s in ((p.get("info") or {}).get("socials") or []):
        if s.get("type") == "twitter":
            proj = s.get("url", "")
    return q, proj


def x_buzz(sym):
    """OPSIONAL & BERBAYAR (X API pay-per-use). Belum diuji. Mati kalau X_BEARER_TOKEN kosong."""
    tok = os.environ.get("X_BEARER_TOKEN")
    if not tok or not sym:
        return None
    today = time.strftime("%Y-%m-%d")
    if XCALLS["day"] != today:
        XCALLS.update(day=today, n=0)
    if XCALLS["n"] >= int(os.environ.get("X_MAX_CALLS_PER_DAY", "20")):
        return None
    XCALLS["n"] += 1
    url = "https://api.x.com/2/tweets/counts/recent?query=" + urllib.parse.quote("$%s -is:retweet" % sym)
    try:
        req = urllib.request.Request(url, headers={"Authorization": "Bearer " + tok})
        with urllib.request.urlopen(req, timeout=15) as r:
            return int((json.load(r).get("meta") or {}).get("total_tweet_count"))
    except urllib.error.HTTPError as e:
        print("X API error: HTTP", e.code)
    except Exception as e:
        print("X API error:", e)
    return None


# ---------- Telegram + pelanggan ----------
def owner():
    return str(os.environ.get("TELEGRAM_CHAT_ID", ""))


def tg(method, params=None, timeout=15):
    tok = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not tok:
        return None
    body = json.dumps(params or {}).encode()
    req = urllib.request.Request("https://api.telegram.org/bot%s/%s" % (tok, method), data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        print("telegram %s error: HTTP %s" % (method, e.code))
        if e.code == 409:
            print("  -> ada program lain yang memakai token bot yang sama. Matikan dulu.")
        return {"ok": False, "error_code": e.code}
    except Exception as e:
        print("telegram %s error: %s" % (method, e))
    return None


def subs_load():
    return load(SUBS, {})


def add_sub(chat, name):
    with LOCK:
        d = subs_load()
        if chat not in d and len(d) >= int(os.environ.get("BOT_MAX_USERS", "200")):
            return False
        d[chat] = dict(name=name, t=time.time())
        SUBS.write_text(json.dumps(d))
    return True


def del_sub(chat):
    with LOCK:
        d = subs_load()
        d.pop(chat, None)
        SUBS.write_text(json.dumps(d))


def send(chat, text):
    r = tg("sendMessage", {"chat_id": chat, "text": text[:4000], "disable_web_page_preview": True})
    if r and r.get("error_code") == 403 and str(chat) != owner():
        del_sub(str(chat))          # pengguna memblokir bot -> hapus dari daftar
    return r


def broadcast(msg):
    print(msg)
    targets = set(subs_load()) | ({owner()} if owner() else set())
    for chat in targets:
        send(chat, msg)
        time.sleep(0.06)            # jaga batas kirim Telegram


HELP = ("Perintah:\n/status - kondisi bot\n/narasi - narasi token yang sedang ramai\n"
        "/posisi - posisi paper terbuka\n/report - hasil paper trading\n"
        "/sinyal - 5 sinyal terakhir\n/stop - berhenti berlangganan\n/help - daftar ini")
ADMIN_HELP = "\nAdmin:\n/users - jumlah pelanggan\n/kirim TEKS - kirim pesan ke semua pelanggan"


def report_text():
    paper = load(PAPER, {})
    closed = [x for x in paper.values() if x["status"] == "closed"]
    wins = [x for x in closed if x["pnl"] > 0]
    out = "Paper: %d total, %d terbuka, %d tutup, %d menang" % (
        len(paper), len(paper) - len(closed), len(closed), len(wins))
    if closed:
        out += "\nRata-rata pnl tertutup: %+.1f%%" % (sum(x["pnl"] for x in closed) / len(closed) * 100)
    return out


def posisi_text():
    rows = [(x["symbol"], x["entry"], x["t"]) for x in load(PAPER, {}).values() if x["status"] == "open"]
    if not rows:
        return "Tidak ada posisi paper yang terbuka."
    return "Posisi paper terbuka:\n" + "\n".join(
        "- %s entry %s (%d menit lalu)" % (s, e, (time.time() - t) / 60) for s, e, t in rows[:15])


def sinyal_text():
    try:
        lines = LOG.read_text().strip().splitlines()[-5:]
    except Exception:
        return "Belum ada sinyal."
    out = []
    for ln in lines:
        try:
            r = json.loads(ln)
            out.append("%s skor %s\n%s" % (r["symbol"], r["score"], r.get("url") or ""))
        except Exception:
            pass
    return "Sinyal terakhir:\n" + "\n\n".join(out) if out else "Belum ada sinyal."


def register(chat, name):
    if not add_sub(chat, name):
        return "Kuota pengguna penuh. Coba lagi nanti."
    return "Kamu berlangganan alert.\n\n" + HELP + "\n\n" + DISCLAIMER


def handle(chat, name, text):
    parts = text.split()
    cmd, arg = parts[0].split("@")[0].lower(), " ".join(parts[1:])
    is_owner = chat == owner()
    subbed = is_owner or chat in subs_load()
    invite = os.environ.get("BOT_INVITE_CODE", "")
    if cmd == "/ping":
        return "pong"
    if cmd in ("/start", "/help"):
        if subbed:
            return "Trenches bot aktif.\n\n" + HELP + (ADMIN_HELP if is_owner else "") + "\n\n" + DISCLAIMER
        if invite:
            return "Bot ini khusus undangan. Kirim: /join KODE"
        return register(chat, name) if cmd == "/start" else "Kirim /start untuk berlangganan.\n\n" + DISCLAIMER
    if cmd == "/join":
        if subbed:
            return "Kamu sudah berlangganan."
        return register(chat, name) if (not invite or arg == invite) else "Kode salah."
    if not subbed:
        return "Kirim /start dulu."
    if cmd == "/stop":
        if is_owner:
            return "Owner tidak bisa berhenti berlangganan."
        del_sub(chat)
        return "Berhenti berlangganan. Kirim /start kalau mau kembali."
    if cmd == "/status":
        return ("Putaran terakhir: %s\nKandidat: %d | pair: %d | lolos: %d\nChain: %s | poll: %ds" % (
            STATE["last"], STATE["cand"], STATE["found"], STATE["passed"], CFG["chain"], CFG["poll"]))
    if cmd == "/narasi":
        return narasi_text()
    if cmd == "/posisi":
        return posisi_text()
    if cmd == "/report":
        return report_text()
    if cmd == "/sinyal":
        return sinyal_text()
    if cmd == "/users" and is_owner:
        return "Pelanggan: %d" % len(subs_load())
    if cmd == "/kirim" and is_owner:
        if not arg:
            return "Pakai: /kirim teks pesan"
        n = 0
        for c in list(subs_load()):
            send(c, "[Pengumuman] " + arg)
            n += 1
            time.sleep(0.06)
        return "Terkirim ke %d pelanggan." % n
    return "Perintah tidak dikenal. Kirim /help."


def listener():
    offset = None
    r = tg("getUpdates", {"offset": -1, "timeout": 0})   # lewati pesan lama
    if r and r.get("result"):
        offset = r["result"][-1]["update_id"] + 1
    while True:
        params = {"timeout": 25}
        if offset is not None:
            params["offset"] = offset
        r = tg("getUpdates", params, timeout=40)
        if not r or not r.get("ok"):
            time.sleep(5)
            continue
        for u in r.get("result", []):
            offset = u["update_id"] + 1
            m = u.get("message") or {}
            ch = m.get("chat") or {}
            text = (m.get("text") or "").strip()
            if not text or ch.get("type") != "private":
                continue                                  # abaikan grup/channel
            chat = str(ch.get("id", ""))
            if time.time() - LASTCMD.get(chat, 0) < 1.5:
                continue                                  # anti-spam
            LASTCMD[chat] = time.time()
            name = (m.get("from") or {}).get("first_name", "")
            send(chat, handle(chat, name, text) if text.startswith("/") else "Kirim /help untuk daftar perintah.")


# ---------- sumber token & saringan ----------
def new_pool_tokens(chain):
    d = get("%s/networks/%s/new_pools" % (GT, GT_NET.get(chain, chain)), gap=2.5) or {}
    out = []
    for p in d.get("data", []):
        try:
            out.append(p["relationships"]["base_token"]["data"]["id"].split("_", 1)[1])
        except Exception:
            pass
    return out


def profile_tokens(chain):
    out = []
    for ep in ("/token-profiles/latest/v1", "/token-boosts/latest/v1"):
        d = get(ep)
        for x in d if isinstance(d, list) else []:
            if x.get("chainId") == chain and x.get("tokenAddress"):
                out.append(x["tokenAddress"])
                if x.get("description"):
                    DESC[x["tokenAddress"]] = str(x["description"])[:300]
    return out


def lookup(addrs, chain):
    best = {}
    for i in range(0, len(addrs), 30):
        d = get("/latest/dex/tokens/" + ",".join(addrs[i:i + 30]), gap=0.5) or {}
        for p in d.get("pairs") or []:
            if p.get("chainId") != chain:
                continue
            a = p["baseToken"]["address"]
            liq = (p.get("liquidity") or {}).get("usd") or 0
            if a not in best or liq > ((best[a].get("liquidity") or {}).get("usd") or 0):
                best[a] = p
    return best


def evaluate(p, c):
    liq = (p.get("liquidity") or {}).get("usd") or 0
    vol = (p.get("volume") or {}).get("h1") or 0
    t = (p.get("txns") or {}).get("h1") or {}
    n = (t.get("buys") or 0) + (t.get("sells") or 0)
    ratio = (t.get("buys") or 0) / n if n else 0
    fdv = p.get("fdv") or 0
    created = p.get("pairCreatedAt") or 0
    age_min = (time.time() * 1000 - created) / 60000 if created else 1e9
    fails = []
    if liq < c["min_liq"]: fails.append("likuiditas rendah")
    if age_min < c["min_age_min"]: fails.append("terlalu baru")
    if age_min > c["max_age_h"] * 60: fails.append("terlalu tua")
    if vol < c["min_vol_h1"]: fails.append("volume kecil")
    if n < c["min_txns_h1"]: fails.append("transaksi sedikit")
    if ratio < c["min_buy_ratio"]: fails.append("rasio buy rendah")
    if fdv and fdv > c["max_fdv"]: fails.append("FDV terlalu besar")
    score = round(min(vol / liq, 5) * 20 + ratio * 40 + min(n / 200, 1) * 40, 1) if liq else 0
    return (not fails), score, fails


def cycle(c):
    seen, paper = load(SEEN, {}), load(PAPER, {})
    open_ = [a for a, x in paper.items() if x["status"] == "open"]
    now = lookup(open_, c["chain"]) if open_ else {}
    for a in open_:
        x, p = paper[a], now.get(a)
        px = float((p or {}).get("priceUsd") or 0)
        if not px:
            continue
        pnl = px / x["entry"] - 1
        if pnl >= c["tp"] or pnl <= c["sl"] or (time.time() - x["t"]) / 3600 > c["max_hold_h"]:
            x.update(status="closed", exit=px, pnl=round(pnl, 3))
            broadcast("[PAPER CLOSE] %s pnl %+.0f%%" % (x["symbol"], pnl * 100))
    addrs = [a for a in dict.fromkeys(new_pool_tokens(c["chain"]) + profile_tokens(c["chain"])) if a not in seen]
    n_open = sum(1 for x in paper.values() if x["status"] == "open")
    found = lookup(addrs, c["chain"])
    record_narr(found)
    reasons, passed = {}, 0
    stamp = time.strftime("%H:%M:%S")
    print("[%s] kandidat baru: %d | data pair ditemukan: %d" % (stamp, len(addrs), len(found)))
    for a, p in found.items():
        if a in seen:
            continue
        ok, score, fails = evaluate(p, c)
        if not ok:
            for f in fails:
                reasons[f] = reasons.get(f, 0) + 1
            continue
        passed += 1
        seen[a] = int(time.time())
        bt = p["baseToken"]
        sym, px = bt.get("symbol", "?"), float(p.get("priceUsd") or 0)
        rec = dict(t=time.time(), addr=a, symbol=sym, score=score, price=px, url=p.get("url"))
        with LOG.open("a") as f:
            f.write(json.dumps(rec) + "\n")
        th = themes_of("%s %s %s" % (bt.get("name", ""), sym, DESC.get(a, "")))
        xq, xproj = x_links(p)
        buzz = x_buzz(sym)
        msg = "[SINYAL] %s skor %s harga %s\n%s" % (sym, score, px, p.get("url"))
        if th:
            msg += "\nNarasi (tebakan): " + ", ".join(th)
        msg += "\nCek di X: " + xq
        if xproj:
            msg += "\nX proyek: " + xproj
        if buzz is not None:
            msg += "\nPost X (7 hari): %d" % buzz
        msg += "\nCek keamanan dulu (honeypot/dev holding). Bukan saran finansial."
        broadcast(msg)
        if px and n_open < c["max_open"] and a not in paper:
            paper[a] = dict(symbol=sym, entry=px, t=time.time(), status="open")
            n_open += 1
    print("  lolos saringan: %d | gagal karena: %s" % (passed, reasons or "-"))
    if not addrs:
        print("  (tidak ada data masuk: cek koneksi internet / API sedang limit)")
    STATE.update(last=stamp, cand=len(addrs), found=len(found), passed=passed)
    PAPER.write_text(json.dumps(paper, indent=1))
    SEEN.write_text(json.dumps(seen))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain", default=CFG["chain"], choices=list(GT_NET))
    ap.add_argument("--poll", type=int, default=CFG["poll"])
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--report", action="store_true")
    a = ap.parse_args()
    if a.report:
        print(report_text())
        return
    CFG.update(chain=a.chain, poll=a.poll)
    if not a.once:
        if os.environ.get("TELEGRAM_BOT_TOKEN") and owner():
            threading.Thread(target=listener, daemon=True).start()
            send(owner(), "Trenches bot aktif. Kamu admin. Kirim /help.")
            mode = "undangan" if os.environ.get("BOT_INVITE_CODE") else "terbuka"
            print("Telegram aktif. Mode pendaftaran: %s. Pelanggan sekarang: %d" % (mode, len(subs_load())))
        else:
            print("Telegram nonaktif (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID belum diisi).")
    try:
        while True:
            cycle(CFG)
            if a.once:
                break
            time.sleep(max(a.poll, 20))
    except KeyboardInterrupt:
        print("\nBerhenti.")


if __name__ == "__main__":
    main()
