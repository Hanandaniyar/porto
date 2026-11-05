#!/usr/bin/env python3
"""trenches_bot.py v4 - screener token baru + ANALISIS KEAMANAN wajib + narasi + bot Telegram multi-user.
Hanya stdlib, Python 3.8+. TIDAK ada wallet/transaksi asli. Semua analisis = penanda risiko, BUKAN jaminan aman.

Sumber data:
  GeckoTerminal + DexScreener   : token baru & metrik (gratis, tanpa key)
  RugCheck (Solana)             : authority, LP lock, top holder, penanda risiko (gratis, tanpa key)
  GMGN via gmgn-cli             : fees (total_fee), honeypot, bundler, rug_ratio (butuh gmgn-cli + API key GMGN)
  Bubblemaps Data API (opsional): skor desentralisasi & bundle (BERBAYAR, 25 kredit/panggilan)

Env: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (admin), BOT_INVITE_CODE, BOT_MAX_USERS,
     BUBBLEMAPS_API_KEY, BUBBLEMAPS_MAX_PER_DAY (default 40), REQUIRE_FEES (default 1),
     X_BEARER_TOKEN, X_MAX_CALLS_PER_DAY.
Pakai: python3 trenches_bot.py [--once] [--report] [--chain solana]
"""
import argparse, json, os, queue, re, shutil, subprocess, threading, time
from concurrent.futures import ThreadPoolExecutor
import urllib.parse, urllib.request, urllib.error
from pathlib import Path

DS = "https://api.dexscreener.com"
GT = "https://api.geckoterminal.com/api/v2"
UA = {"User-Agent": "trenches-bot/5.1", "Accept": "application/json"}
DATA = Path(os.environ.get("BOT_DATA", "./botdata"))
DATA.mkdir(exist_ok=True)
SEEN, PAPER, LOG = DATA / "seen.json", DATA / "paper.json", DATA / "signals.jsonl"
SUBS, NARR = DATA / "subscribers.json", DATA / "narratives.json"
LOCK = threading.Lock()

CFG = dict(
    chain="solana", poll=20,
    min_liq=8000, min_age_min=5, max_age_h=6,
    min_vol_h1=5000, min_txns_h1=40, min_buy_ratio=0.55, max_fdv=3_000_000,
    min_fees_sol=10,                 # fees yang beredar minimal (SOL)
    recheck_min=20, max_analyze=8, workers=3,   # hemat kuota; analisis berjalan paralel
    max_pump_h1=400, min_m5_change=-12, min_liq_mcap=0.04,   # hindari token yang sudah terbang / sedang jatuh / likuiditas tipis
    min_score=float(os.environ.get("MIN_SCORE", "120")),   # skor akhir (momentum + percepatan + narasi) minimal untuk alert
    tp=1.0, sl=-0.4, max_hold_h=24, max_open=10,
)
GT_NET = {"solana": "solana", "bsc": "bsc", "base": "base", "ethereum": "eth"}
_last = [0.0]
STATE = {"last": "belum ada putaran", "cand": 0, "found": 0, "passed": 0}
DESC, LASTCMD, REJ, BM_CACHE = {}, {}, {}, {}
XCALLS, BMCALLS = {"day": "", "n": 0}, {"day": "", "n": 0}
ICON = {"ok": "\u2705", "warn": "\u26a0\ufe0f", "fail": "\u274c", "na": "\u2754"}
ADDR_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

DISCLAIMER = ("PERINGATAN: ini screener otomatis, BUKAN saran finansial. Analisis hanya penanda risiko, "
              "bukan jaminan aman. Token baru sangat berisiko (rug/honeypot). Jangan pakai uang yang tak siap hilang.")

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


GENERIC = set("coin token the of and on in to for sol solana pump fun new official finance protocol swap dao meta go of by is it my".split())
FALSE_POS = set("catch category catalog cattle catholic dogma dogged aim air aid aisle pupil puppet bearing beard apex "
                "bullet goatee melon watermelon devote muskrat".split())

# ---------- HTTP & util ----------
_hosts, _hlock = {}, threading.Lock()
_cache, _clock = {}, threading.Lock()


def get(url, gap=1.2, headers=None):
    """GET JSON. Jeda dihitung PER HOST dan aman untuk thread: host berbeda berjalan paralel."""
    if url.startswith("/"):
        url = DS + url
    host = urllib.parse.urlparse(url).netloc
    with _hlock:
        slot = max(time.time(), _hosts.get(host, 0.0))
        _hosts[host] = slot + gap
    wait = slot - time.time()
    if wait > 0:
        time.sleep(wait)
    h = dict(UA)
    h.update(headers or {})
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=20) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print("429 rate limit dari %s, host itu dijeda 60 detik" % host)
            with _hlock:
                _hosts[host] = time.time() + 60
        else:
            print("HTTP", e.code, url[:70])
    except Exception as e:
        print("error jaringan:", e)
    return None


def cached(key, ttl, fn):
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    val = fn()
    if val is not None:                      # kegagalan tidak di-cache
        with _clock:
            _cache[key] = (time.time(), val)
    return val


def load(path, default):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def num(x):
    try:
        return float(x)
    except Exception:
        return None


def truthy(x):
    if x is None:
        return None
    if isinstance(x, str):
        return x.strip().lower() in ("1", "true", "yes")
    return bool(x)


def pick(obj, *names):
    """Cari nilai pertama untuk salah satu nama kunci (rekursif, tahan perbedaan struktur)."""
    names, queue = set(names), [obj]
    while queue:
        o = queue.pop(0)
        if isinstance(o, dict):
            for k, v in o.items():
                if k in names and v not in (None, ""):
                    return v
            queue.extend(o.values())
        elif isinstance(o, list):
            queue.extend(o)
    return None


def pct(x):
    return "%.0f%%" % (x * 100)


# ---------- sumber analisis ----------
def gmgn(cmd, addr):
    """GMGN lewat gmgn-cli (butuh terpasang + API key). Hasil di-cache 10 menit."""
    if not shutil.which("gmgn-cli"):
        return None

    def run():
        try:
            out = subprocess.run(["gmgn-cli", "token", cmd, "--chain", "sol", "--address", addr, "--raw"],
                                 capture_output=True, text=True, timeout=30)
            return json.loads(out.stdout)
        except Exception as e:
            print("gmgn-cli %s gagal: %s" % (cmd, e))
            return None
    return cached(("gmgn", cmd, addr), 600, run)


def rugcheck(addr):
    def run():
        d = get("https://api.rugcheck.xyz/v1/tokens/%s/report" % addr, gap=1.0)
        return d if isinstance(d, dict) else None
    return cached(("rc", addr), 600, run)


def bubblemaps(addr):
    key = os.environ.get("BUBBLEMAPS_API_KEY")
    if not key:
        return None
    if addr in BM_CACHE:
        return BM_CACHE[addr]
    today = time.strftime("%Y-%m-%d")
    if BMCALLS["day"] != today:
        BMCALLS.update(day=today, n=0)
    if BMCALLS["n"] >= int(os.environ.get("BUBBLEMAPS_MAX_PER_DAY", "40")):
        return None
    BMCALLS["n"] += 1
    d = get("https://api.bubblemaps.io/v0/tokens/metrics/solana/%s" % addr, gap=1.0, headers={"X-ApiKey": key})
    BM_CACHE[addr] = d if isinstance(d, dict) else None
    return BM_CACHE[addr]


def tools_status():
    return "GMGN CLI: %s | RugCheck: aktif | Bubblemaps API: %s" % (
        "aktif" if shutil.which("gmgn-cli") else "TIDAK ADA",
        "aktif" if os.environ.get("BUBBLEMAPS_API_KEY") else "nonaktif")


# ---------- ANALISIS KEAMANAN ----------
def analyze_full(addr, p, c):
    """Return list (status, label, detail). status: ok | warn | fail | na."""
    rows = []
    m = {}                                     # angka mentah untuk mesin (engine.py)

    def add(st, label, detail):
        rows.append((st, label, detail))

    with ThreadPoolExecutor(max_workers=3) as ex:
        f1, f2, f3 = ex.submit(gmgn, "security", addr), ex.submit(gmgn, "info", addr), ex.submit(rugcheck, addr)
        sec, info, rc = f1.result(), f2.result(), f3.result()
    gm = [x for x in (sec, info) if x]
    risks = (rc or {}).get("risks") or []
    rtext = " ".join(("%s %s" % (r.get("name", ""), r.get("description", ""))).lower() for r in risks)
    liq = (p.get("liquidity") or {}).get("usd") or 0
    vol = (p.get("volume") or {}).get("h1") or 0
    tx = (p.get("txns") or {}).get("h1") or {}
    ntx = (tx.get("buys") or 0) + (tx.get("sells") or 0)

    # 1) Fees yang beredar (Solana)
    fee = num(pick(gm, "total_fee"))
    if fee is not None and fee > 1e5:
        fee = fee / 1e9                       # kemungkinan lamports
    m["fees_sol"] = fee
    need = os.environ.get("REQUIRE_FEES", "1") != "0"
    if fee is None:
        add("fail" if need else "na", "Fees beredar", "tidak terbaca (butuh gmgn-cli + API key GMGN)")
    elif fee >= c["min_fees_sol"]:
        add("ok", "Fees beredar", "%.1f SOL (minimal %g)" % (fee, c["min_fees_sol"]))
    else:
        add("fail", "Fees beredar", "%.1f SOL, di bawah minimal %g" % (fee, c["min_fees_sol"]))

    # 2) Honeypot
    hp = truthy(pick(gm, "is_honeypot"))
    sell, buy = num(pick(gm, "sell_tax")), num(pick(gm, "buy_tax"))
    m["honeypot"], m["sell_tax"] = hp, sell
    tf = (rc or {}).get("transferFee")
    tfp = num(tf.get("pct")) if isinstance(tf, dict) else None
    if hp or "honeypot" in rtext:
        add("fail", "Honeypot", "terdeteksi penanda honeypot")
    elif (sell or 0) > 0.10 or (tfp or 0) >= 10:
        add("fail", "Honeypot", "pajak/biaya transfer tinggi (jual %s, transfer %s%%)" % (
            pct(sell) if sell is not None else "?", tfp if tfp is not None else "?"))
    elif hp is None and rc is None:
        add("na", "Honeypot", "tidak ada data")
    elif hp is None:
        add("warn", "Honeypot", "RugCheck tak menandai, tapi tanpa data GMGN (pajak jual tak diketahui)")
    else:
        add("ok", "Honeypot", "tak ada penanda; pajak jual %s. Catatan: simulasi jual asli tidak tersedia gratis" % (
            pct(sell) if sell is not None else "tak diketahui"))

    # 3) Mint & freeze authority
    if rc is not None:
        mint_on = bool(rc.get("mintAuthority") or (rc.get("token") or {}).get("mintAuthority"))
        frz_on = bool(rc.get("freezeAuthority") or (rc.get("token") or {}).get("freezeAuthority"))
    else:
        rm, rf = truthy(pick(gm, "renounced_mint")), truthy(pick(gm, "renounced_freeze_account"))
        mint_on = None if rm is None else (not rm)
        frz_on = None if rf is None else (not rf)
    m["mint_on"], m["freeze_on"] = mint_on, frz_on
    if mint_on or frz_on:
        on = [n for n, v in (("mint", mint_on), ("freeze", frz_on)) if v]
        add("fail", "Mint/Freeze", "authority masih aktif: " + ", ".join(on))
    elif mint_on is None and frz_on is None:
        add("na", "Mint/Freeze", "tidak ada data")
    else:
        add("ok", "Mint/Freeze", "mint & freeze authority sudah dilepas")

    # 4) Likuiditas dikunci / dibakar
    locks = []
    for m in (rc or {}).get("markets") or []:
        v = num(((m or {}).get("lp") or {}).get("lpLockedPct"))
        if v is not None:
            locks.append(v)
    if str(pick(gm, "burn_status") or "").lower() in ("burn", "burned"):
        locks.append(100.0)
    lr = num(pick(gm, "locked_ratio"))
    if lr is not None:
        locks.append(lr * 100)
    m["lp_locked_pct"] = max(locks) if locks else None
    if not locks:
        add("na", "LP terkunci/terbakar", "tidak ada data")
    else:
        best = max(locks)
        st = "ok" if best >= 80 else ("warn" if best >= 50 else "fail")
        add(st, "LP terkunci/terbakar", "%.0f%% (aman jika >=80%%)" % best)

    # 5) Konsentrasi holder (dev, sniper, top 10)
    top10 = num(pick(gm, "top_10_holder_rate"))
    from_rc = False
    if top10 is None and rc and rc.get("topHolders"):
        known, skip = rc.get("knownAccounts") or {}, set()
        if isinstance(known, dict):                      # buang akun pool/AMM supaya tidak dihitung sebagai holder
            for k, v in known.items():
                s = ("%s %s" % ((v or {}).get("type", ""), (v or {}).get("name", ""))).lower()
                if any(w in s for w in ("amm", "pool", "liquidity", "bonding", "vault")):
                    skip.add(k)
        hs = [h for h in rc["topHolders"] if h.get("address") not in skip and h.get("owner") not in skip]
        top10 = sum(num(h.get("pct")) or 0 for h in hs[:10]) / 100.0
        from_rc = True
    d1, d2 = num(pick(gm, "creator_hold_rate")), num(pick(gm, "dev_team_hold_rate"))
    dev = None if (d1 is None and d2 is None) else (d1 or 0) + (d2 or 0)
    snip = num(pick(gm, "top70_sniper_hold_rate"))
    m["top10"], m["dev"], m["sniper"] = top10, dev, snip
    parts = []
    if top10 is not None:
        parts.append("top10 %s%s" % (pct(top10), " (RugCheck, pool/AMM dibuang jika terdeteksi)" if from_rc else ""))
    if dev is not None:
        parts.append("dev %s" % pct(dev))
    if snip is not None:
        parts.append("sniper %s" % pct(snip))
    if top10 is None and dev is None and snip is None:
        add("na", "Konsentrasi holder", "tidak ada data")
    elif (top10 or 0) > 0.40 or (dev or 0) > 0.10 or (snip or 0) > 0.30:
        add("fail", "Konsentrasi holder", ", ".join(parts) + " (terlalu terpusat)")
    elif (top10 or 0) > 0.25 or (dev or 0) > 0.05 or (snip or 0) > 0.15:
        add("warn", "Konsentrasi holder", ", ".join(parts) + " (cukup terpusat)")
    else:
        add("ok", "Konsentrasi holder", ", ".join(parts))

    # 6) Bubbles / klaster wallet
    bm = bubblemaps(addr)
    if bm:
        ss, sc = bm.get("supply_stats") or {}, bm.get("scores") or {}
        bundles, score, fresh = num(ss.get("bundles")), num(sc.get("bubblemaps_score")), num(ss.get("fresh_wallets"))
        m["bundler"] = bundles
        det = "Bubblemaps: bundle %s, skor %s, wallet baru %s" % (
            pct(bundles) if bundles is not None else "?", "%.0f" % score if score is not None else "?",
            pct(fresh) if fresh is not None else "?")
        if (bundles or 0) > 0.35 or (score is not None and score < 35):
            add("fail", "Bubbles", det + " (klaster besar/terpusat)")
        elif (bundles or 0) > 0.20 or (score is not None and score < 50) or (fresh or 0) > 0.30:
            add("warn", "Bubbles", det)
        else:
            add("ok", "Bubbles", det)
    else:
        bund = num(pick(gm, "bundler_rate", "top_bundler_trader_percentage"))
        m["bundler"] = bund
        ins = (rc or {}).get("graphInsidersDetected") or len((rc or {}).get("insiderNetworks") or [])
        if bund is None and not ins:
            add("na", "Bubbles", "tidak ada data bundler/klaster; cek manual di Bubblemaps")
        elif (bund or 0) > 0.30:
            add("fail", "Bubbles", "bundler %s (indikasi supply dibundel)" % pct(bund))
        elif (bund or 0) > 0.15 or ins:
            add("warn", "Bubbles", "bundler %s, insider terdeteksi: %s (perkiraan tanpa Bubblemaps API)" % (
                pct(bund) if bund is not None else "?", ins or 0))
        else:
            add("ok", "Bubbles", "bundler %s, insider 0 (perkiraan tanpa Bubblemaps API)" % (
                pct(bund) if bund is not None else "?"))

    # 7) Volume palsu (wash trading)
    wt = truthy(pick(gm, "is_wash_trading"))
    ratio = vol / liq if liq else 0
    m["vol_liq_ratio"] = ratio
    avg = vol / ntx if ntx else 0
    if wt:
        add("fail", "Wash trading", "GMGN menandai wash trading")
    elif ratio > 40 or (ntx >= 100 and avg < 3):
        add("warn", "Wash trading", "curiga: volume/likuiditas %.0fx, rata-rata transaksi $%.1f" % (ratio, avg))
    else:
        add("ok", "Wash trading", "volume/likuiditas %.1fx, rata-rata transaksi $%.1f" % (ratio, avg))

    # 8) Rekam jejak pembuat
    rug_ratio = num(pick(gm, "rug_ratio"))
    m["rug_ratio"] = rug_ratio
    cstat = pick(gm, "creator_token_status")
    crisk = [r for r in risks if "creator" in (r.get("name", "") + r.get("description", "")).lower()
             and str(r.get("level", "")).lower() in ("danger", "warn", "warning")]
    if (rc or {}).get("rugged") or (rug_ratio or 0) > 0.30:
        add("fail", "Rekam jejak pembuat", "ditandai rug (rug_ratio %s, rugged=%s)" % (
            rug_ratio if rug_ratio is not None else "?", (rc or {}).get("rugged")))
    elif crisk or (rug_ratio or 0) > 0.15 or str(cstat).lower() == "creator_close":
        names = ", ".join(r.get("name", "?") for r in crisk) or "-"
        add("warn", "Rekam jejak pembuat", "rug_ratio %s, status dev %s, penanda: %s" % (
            rug_ratio if rug_ratio is not None else "?", cstat or "?", names))
    elif rug_ratio is None:
        add("na", "Rekam jejak pembuat", "tak ditandai rug oleh RugCheck, tapi tanpa data GMGN; riwayat lengkap tak tersedia gratis"
            if rc is not None else "tidak ada data")
    else:
        add("ok", "Rekam jejak pembuat", "rug_ratio %s, status dev %s (riwayat lengkap tak bisa diverifikasi)" % (
            rug_ratio if rug_ratio is not None else "?", cstat or "?"))
    return rows, m


def analyze(addr, p, c):
    return analyze_full(addr, p, c)[0]


def verdict(rows):
    fails = [l for st, l, d in rows if st == "fail"]
    warns = sum(1 for st, _, _ in rows if st == "warn")
    nas = sum(1 for st, _, _ in rows if st == "na")
    if fails:
        return "DITOLAK: " + ", ".join(fails)
    if warns >= 2 or nas >= 2:
        return "Lolos cek wajib, tapi %d peringatan & %d data kosong. Risiko SEDANG." % (warns, nas)
    return "Lolos semua cek yang bisa dilakukan. Tetap berisiko, bukan jaminan aman."


def format_rows(rows):
    return "\n".join("%s %s: %s" % (ICON[st], label, detail) for st, label, detail in rows)


def manual_links(addr, p):
    sym = (p.get("baseToken") or {}).get("symbol", "")
    return ("Cek manual:\nBubblemaps https://app.bubblemaps.io/sol/token/%s\nRugCheck https://rugcheck.xyz/tokens/%s\nX https://x.com/search?q=%s&f=live"
            % (addr, addr, urllib.parse.quote("$" + sym)))


def cek_text(addr, c):
    if c["chain"] != "solana":
        return "Analisis keamanan saat ini hanya untuk chain Solana."
    if not ADDR_RE.match(addr):
        return "Alamat token tidak valid. Pakai: /cek ALAMAT_TOKEN_SOLANA"
    p = lookup([addr], c["chain"]).get(addr)
    if not p:
        return "Token tidak ditemukan di DexScreener (Solana)."
    rows = analyze(addr, p, c)
    sym = (p.get("baseToken") or {}).get("symbol", "?")
    return "ANALISIS %s\n%s\n\n%s\n\n%s\n\n%s" % (sym, format_rows(rows), verdict(rows), manual_links(addr, p), DISCLAIMER)


# ---------- narasi ----------
def split_words(text):
    t = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)          # CatWifHat -> Cat Wif Hat
    return re.findall(r"[a-z0-9]+", t.lower())


def kw_match(word, kw):
    if word == kw or word == kw + "s":
        return True
    return (len(kw) >= 3 and (word.startswith(kw) or word.endswith(kw))
            and len(word) <= len(kw) + 10 and word not in FALSE_POS)


def themes_of(text):
    words = split_words(text)
    return [n for n, kws in THEMES.items() if any(kw_match(w, k) for w in words for k in kws)]


def name_words(name, sym):
    return [w for w in dict.fromkeys(split_words("%s %s" % (name, sym)))
            if len(w) >= 3 and w not in GENERIC and not w.isdigit()][:8]


def compute_heat():
    """Agregasi narasi 3 jam terakhir: (tema -> stat, kata berulang -> stat)."""
    now, themes, words = time.time(), {}, {}
    for v in load(NARR, {}).values():
        if now - v["t"] > 3 * 3600:
            continue
        for th in v["th"]:
            g = themes.setdefault(th, dict(n=0, vol=0, top=("", 0)))
            g["n"] += 1
            g["vol"] += v["vol"]
            if v["vol"] > g["top"][1]:
                g["top"] = (v["sym"], v["vol"])
        for w in set(v.get("w", [])):
            g = words.setdefault(w, dict(n=0, vol=0))
            g["n"] += 1
            g["vol"] += v["vol"]
    return themes, {w: g for w, g in words.items() if g["n"] >= 3}


def narrative_bonus(th, ws, heat):
    """Bonus skor (maks 30) untuk token yang ikut narasi yang memang sedang ramai (>=3 token)."""
    themes, hot_w = heat
    rank = sorted(themes, key=lambda k: -themes[k]["vol"])
    bonus, tags = 0, []
    for t in th:
        g = themes.get(t)
        if not g or g["n"] < 3:
            continue
        i = rank.index(t)
        bonus = max(bonus, (20, 12, 6)[i] if i < 3 else 3)
        tags.append("%s (%d token, vol $%s)" % (t, g["n"], format(int(g["vol"]), ",")))
    hw = sorted((w for w in set(ws) if w in hot_w), key=lambda w: -hot_w[w]["n"])[:3]
    if hw:
        bonus += 10
        tags.append("kata ramai: " + ", ".join("%s (%d token)" % (w, hot_w[w]["n"]) for w in hw))
    return min(bonus, 30), tags


def clone_info(sym, a):
    """True jika simbol sama dipakai >=3 token dalam 6 jam dan token ini BUKAN yang volumenya terbesar."""
    s, now = sym.strip().lower(), time.time()
    same = [(v["vol"], k) for k, v in load(NARR, {}).items()
            if v["sym"].strip().lower() == s and now - v["t"] < 6 * 3600]
    return len(same) >= 3 and max(same)[1] != a


def record_narr(found):
    now = time.time()
    with LOCK:
        db = load(NARR, {})
        for a, p in found.items():
            vol = (p.get("volume") or {}).get("h1") or 0
            if vol < 1000:
                continue
            bt = p.get("baseToken") or {}
            text = "%s %s %s" % (bt.get("name", ""), bt.get("symbol", ""), DESC.get(a, ""))
            db[a] = dict(t=now, sym=bt.get("symbol", "?"), vol=vol, th=themes_of(text),
                         w=name_words(bt.get("name", ""), bt.get("symbol", "")))
        db = {a: v for a, v in db.items() if now - v["t"] < 6 * 3600}
        NARR.write_text(json.dumps(db))


def narasi_text():
    themes, hot_w = compute_heat()
    if not themes and not hot_w:
        return "Belum ada narasi terdeteksi (3 jam terakhir)."
    out = ["Narasi paling ramai (3 jam, dari nama/deskripsi token baru):"]
    for th, g in sorted(themes.items(), key=lambda kv: -kv[1]["vol"])[:6]:
        out.append("- %s: %d token, vol $%s, teratas %s" % (th, g["n"], format(int(g["vol"]), ","), g["top"][0]))
    if hot_w:
        out.append("\nKata yang berulang di nama token (minimal 3 token):")
        for w, g in sorted(hot_w.items(), key=lambda kv: -kv[1]["n"])[:6]:
            out.append("- %s: %d token, vol $%s" % (w, g["n"], format(int(g["vol"]), ",")))
    out.append("\nIni dari nama/deskripsi token, bukan data viral X. Narasi ramai juga bisa berarti banyak kloning. Cek X manual.")
    return "\n".join(out)


def x_buzz(sym):
    """OPSIONAL & BERBAYAR (X API). Belum diuji. Mati kalau X_BEARER_TOKEN kosong."""
    tok = os.environ.get("X_BEARER_TOKEN")
    if not tok or not sym:
        return None
    today = time.strftime("%Y-%m-%d")
    if XCALLS["day"] != today:
        XCALLS.update(day=today, n=0)
    if XCALLS["n"] >= int(os.environ.get("X_MAX_CALLS_PER_DAY", "20")):
        return None
    XCALLS["n"] += 1
    d = get("https://api.x.com/2/tweets/counts/recent?query=" + urllib.parse.quote("$%s -is:retweet" % sym),
            gap=1.0, headers={"Authorization": "Bearer " + tok})
    try:
        return int((d.get("meta") or {}).get("total_tweet_count"))
    except Exception:
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
        del_sub(str(chat))
    return r


OUTQ = queue.Queue()
_sender = []


def _send_loop():
    while True:
        chat, msg = OUTQ.get()
        try:
            send(chat, msg)
            time.sleep(0.05)
        finally:
            OUTQ.task_done()


def broadcast(msg):
    print(msg)
    if not _sender:
        t = threading.Thread(target=_send_loop, daemon=True)
        t.start()
        _sender.append(t)
    for chat in set(subs_load()) | ({owner()} if owner() else set()):
        OUTQ.put((chat, msg))


HELP = ("Perintah:\n/cek ALAMAT - analisis keamanan token Solana\n/status - kondisi bot\n"
        "/narasi - narasi token yang sedang ramai\n/posisi - posisi paper terbuka\n"
        "/report - hasil paper trading\n/sinyal - 5 sinyal terakhir\n/stop - berhenti berlangganan\n/help - daftar ini")
ADMIN_HELP = "\nAdmin:\n/users - jumlah pelanggan\n/kirim TEKS - kirim pesan ke semua pelanggan"
EXTRA_CMDS = {}                 # perintah tambahan khusus owner (didaftarkan engine.py)


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


def is_subbed(chat):
    return chat == owner() or chat in subs_load()


def handle(chat, name, text):
    parts = text.split()
    cmd, arg = parts[0].split("@")[0].lower(), " ".join(parts[1:])
    is_owner = chat == owner()
    subbed = is_subbed(chat)
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
        return ("Putaran terakhir: %s\nKandidat: %d | pair: %d | lolos: %d | durasi %.1fs\nChain: %s | poll: %ds\n%s" % (
            STATE["last"], STATE["cand"], STATE["found"], STATE["passed"], STATE.get("dur", 0),
            CFG["chain"], CFG["poll"], tools_status()))
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
    fn = EXTRA_CMDS.get(cmd)
    if fn and is_owner:
        return fn(arg)
    return "Perintah tidak dikenal. Kirim /help."


def cek_worker(chat, arg):
    send(chat, "Menganalisis token... (bisa 10-40 detik)")
    try:
        send(chat, cek_text(arg.strip(), CFG))
    except Exception as e:
        print("cek error:", e)
        send(chat, "Analisis gagal. Coba lagi nanti.")


def listener():
    offset = None
    r = tg("getUpdates", {"offset": -1, "timeout": 0})
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
                continue
            chat = str(ch.get("id", ""))
            cool = 15 if text.lower().startswith("/cek") else 1.5
            if time.time() - LASTCMD.get(chat, {}).get(text.split()[0].lower(), 0) < cool:
                continue
            LASTCMD.setdefault(chat, {})[text.split()[0].lower()] = time.time()
            name = (m.get("from") or {}).get("first_name", "")
            if text.lower().split("@")[0].startswith("/cek"):
                if not is_subbed(chat):
                    send(chat, "Kirim /start dulu.")
                else:
                    arg = " ".join(text.split()[1:])
                    threading.Thread(target=cek_worker, args=(chat, arg), daemon=True).start()
                continue
            send(chat, handle(chat, name, text) if text.startswith("/") else "Kirim /help untuk daftar perintah.")


# ---------- sumber token & saringan momentum ----------
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
    vm5 = (p.get("volume") or {}).get("m5") or 0
    t = (p.get("txns") or {}).get("h1") or {}
    n = (t.get("buys") or 0) + (t.get("sells") or 0)
    ratio = (t.get("buys") or 0) / n if n else 0
    fdv = p.get("fdv") or 0
    mcap = p.get("marketCap") or fdv
    pc = p.get("priceChange") or {}
    h1, m5 = num(pc.get("h1")), num(pc.get("m5"))
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
    if h1 is not None and h1 > c["max_pump_h1"]: fails.append("sudah terlalu naik")
    if m5 is not None and m5 < c["min_m5_change"]: fails.append("sedang turun")
    if mcap and liq / mcap < c["min_liq_mcap"]: fails.append("likuiditas tipis vs mcap")
    accel = (vm5 * 12) / vol if vol else 0        # >1 berarti 5 menit terakhir lebih ramai dari rata-rata jam ini
    score = round(min(vol / liq, 5) * 20 + ratio * 40 + min(n / 200, 1) * 40 + min(accel, 3) * 10, 1) if liq else 0
    return (not fails), score, fails


def cycle(c):
    t0 = time.time()
    seen, paper = load(SEEN, {}), load(PAPER, {})
    open_ = [a for a, x in paper.items() if x["status"] == "open"]
    with ThreadPoolExecutor(max_workers=3) as ex:        # tiga sumber data diambil bersamaan
        f_pool = ex.submit(new_pool_tokens, c["chain"])
        f_prof = ex.submit(profile_tokens, c["chain"])
        f_open = ex.submit(lookup, open_, c["chain"]) if open_ else None
        pool, prof = f_pool.result(), f_prof.result()
        now = f_open.result() if f_open else {}
    for a in open_:
        x, p = paper[a], now.get(a)
        px = float((p or {}).get("priceUsd") or 0)
        if not px:
            continue
        pnl = px / x["entry"] - 1
        if pnl >= c["tp"] or pnl <= c["sl"] or (time.time() - x["t"]) / 3600 > c["max_hold_h"]:
            x.update(status="closed", exit=px, pnl=round(pnl, 3))
            broadcast("[PAPER CLOSE] %s pnl %+.0f%%" % (x["symbol"], pnl * 100))
    addrs = [a for a in dict.fromkeys(pool + prof) if a not in seen]
    n_open = sum(1 for x in paper.values() if x["status"] == "open")
    found = lookup(addrs, c["chain"])
    record_narr(found)
    heat = compute_heat()
    only_narr = os.environ.get("ONLY_NARRATIVE") == "1"
    reasons = {}
    stamp = time.strftime("%H:%M:%S")
    print("[%s] kandidat baru: %d | data pair ditemukan: %d" % (stamp, len(addrs), len(found)))

    def count(label):
        reasons[label] = reasons.get(label, 0) + 1

    # tahap 1: momentum + narasi + anti-kloning, lalu urut dari skor tertinggi
    cands = []
    for a, p in found.items():
        if a in seen:
            continue
        ok, score, fails = evaluate(p, c)
        if not ok:
            for f in fails:
                count(f)
            continue
        bt = p["baseToken"]
        sym, name = bt.get("symbol", "?"), bt.get("name", "")
        th = themes_of("%s %s %s" % (name, sym, DESC.get(a, "")))
        bonus, tags = narrative_bonus(th, name_words(name, sym), heat)
        if clone_info(sym, a):
            count("kloning simbol")
            continue
        if only_narr and not tags:
            count("tanpa narasi panas")
            continue
        total = round(score + bonus, 1)
        if total < c["min_score"]:
            count("skor rendah")
            continue
        cands.append((total, a, p, th, tags, bonus))
    cands.sort(key=lambda x: -x[0])

    # tahap 2: analisis keamanan wajib, paralel
    results = {}
    if c["chain"] == "solana":
        todo = []
        for item in cands:
            ts, labels = REJ.get(item[1], (0, None))
            if labels is not None and time.time() - ts < c["recheck_min"] * 60:
                for l in labels:
                    count("keamanan: " + l)
            elif len(todo) >= c["max_analyze"]:
                count("antrean analisis penuh")
            else:
                todo.append(item)
        with ThreadPoolExecutor(max_workers=c["workers"]) as ex:
            futs = {item[1]: ex.submit(analyze, item[1], item[2], c) for item in todo}
            for a, f in futs.items():
                try:
                    results[a] = f.result()
                except Exception as e:
                    print("analisis gagal:", e)
        final = [item for item in todo if item[1] in results]
    else:
        final = cands

    passed = 0
    for total, a, p, th, tags, bonus in final:
        if c["chain"] == "solana":
            rows = results[a]
            bad = [l for st, l, d in rows if st == "fail"]
            if bad:
                REJ[a] = (time.time(), bad)
                for l in bad:
                    count("keamanan: " + l)
                continue
            analysis = "ANALISIS KEAMANAN (otomatis, bukan jaminan):\n%s\n%s\n\n%s" % (
                format_rows(rows), verdict(rows), manual_links(a, p))
        else:
            analysis = "Analisis keamanan belum tersedia untuk chain ini. Cek manual."
        passed += 1
        seen[a] = int(time.time())
        bt = p["baseToken"]
        sym, px = bt.get("symbol", "?"), float(p.get("priceUsd") or 0)
        with LOG.open("a") as f:
            f.write(json.dumps(dict(t=time.time(), addr=a, symbol=sym, score=total, bonus=bonus,
                                    price=px, url=p.get("url"))) + "\n")
        buzz = x_buzz(sym)
        msg = "[SINYAL] %s skor %s%s harga %s\n%s" % (
            sym, total, " (narasi +%d)" % bonus if bonus else "", px, p.get("url"))
        if tags:
            msg += "\nNarasi: " + "; ".join(tags)
        elif th:
            msg += "\nNarasi (tebakan, belum ramai): " + ", ".join(th)
        if buzz is not None:
            msg += "\nPost X (7 hari): %d" % buzz
        msg += "\n\n" + analysis + "\n\nBukan saran finansial."
        broadcast(msg)
        if px and n_open < c["max_open"] and a not in paper:
            paper[a] = dict(symbol=sym, entry=px, t=time.time(), status="open")
            n_open += 1
    dur = time.time() - t0
    print("  lolos semua saringan: %d | durasi %.1fs | gagal karena: %s" % (passed, dur, reasons or "-"))
    if not addrs:
        print("  (tidak ada data masuk: cek koneksi internet / API sedang limit)")
    STATE.update(last=stamp, cand=len(addrs), found=len(found), passed=passed, dur=dur)
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
    print(tools_status())
    if CFG["chain"] == "solana" and not shutil.which("gmgn-cli") and os.environ.get("REQUIRE_FEES", "1") != "0":
        print("PERINGATAN: gmgn-cli tidak ada, syarat fees >= %g SOL tidak bisa dicek -> SEMUA token akan ditolak.\n"
              "  Pasang gmgn-cli + API key GMGN, atau jalankan dengan REQUIRE_FEES=0 (syarat fees dilewati)." % CFG["min_fees_sol"])
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
                OUTQ.join()
                break
            time.sleep(max(a.poll, 15))
    except KeyboardInterrupt:
        print("\nBerhenti.")


if __name__ == "__main__":
    main()
