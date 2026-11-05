#!/usr/bin/env python3
"""engine.py - mesin DRY RUN: trigger -> filter -> security -> entry (SIMULASI) -> exit -> monitor/replay.

TIDAK ADA transaksi asli, wallet, atau private key di file ini. Entry/exit memakai kuotasi Jupiter sebagai
harga fill simulasi. Semua angka strategi ada di config.ini (dibuat otomatis, isinya hanya PLACEHOLDER).
Butuh trenches_bot.py di folder yang sama (data, analisis keamanan, Telegram). Hanya stdlib, Python 3.8+.

  python3 engine.py init                 # buat config.ini
  python3 engine.py run [--with-alerts]  # jalankan monitor + dry run (+ bot alert publik di proses yang sama)
  python3 engine.py report               # statistik monitor & hasil dry run
  python3 engine.py replay --set trail_distance=5,10,20 --set stop_loss_pct=-30,-50   # uji aturan exit di data rekaman
Env: SOLANA_RPC_URL (trigger migrasi & cek Token-2022), JUPITER_API_KEY (opsional), TELEGRAM_* (lihat trenches_bot.py)
"""
import argparse, configparser, itertools, json, os, queue, random, sqlite3, statistics, threading, time
import urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import trenches_bot as tb

CFG_PATH = Path(os.environ.get("ENGINE_CONFIG", "config.ini"))
DB_PATH = tb.DATA / "engine.db"
WSOL = "So11111111111111111111111111111111111111112"
STOP = threading.Event()
DBL, ENTRY_LOCK = threading.RLock(), threading.Lock()
EVENTS = queue.Queue()
RT = dict(paused=False, pause_until=0.0, streak=0)
CFGOBJ = [None]
num = tb.num

DEFAULT_INI = """\
# config.ini - SEMUA angka strategi ada di sini. Nilai di bawah hanyalah PENGISI AWAL (placeholder), BUKAN rekomendasi.
# Ubah lalu restart engine. Mode: DRY RUN (tidak ada transaksi asli).

[trigger]
gt_trending = true
gt_trending_poll_s = 30
gt_new_pools = false
dex_boost = true               # kelompok pembanding (promosi berbayar)
pump_migration = false         # butuh env SOLANA_RPC_URL
pump_migration_account = 39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg
pump_poll_s = 3

[monitor]
warmup_s = 45                  # tunggu sebelum filter, supaya perubahan harga 1 menit bisa dihitung
pair_wait_s = 90               # tunggu DexScreener mengindeks pair baru
snap_fast_s = 5                # 10 menit pertama
snap_mid_s = 30                # sampai 60 menit
snap_slow_s = 60               # sampai track_minutes
track_minutes = 240
pipeline_workers = 8

[filter]
min_market_cap = 20000
max_market_cap = 3000000
min_liquidity_usd = 8000
min_volume_h1 = 5000
min_age_min = 5
max_age_min = 360
min_price_change_1m = -10
min_price_change_5m = -12
max_price_change_5m = 400
max_dev_holding_pct = 10
max_bundler_pct = 30
max_sniper_pct = 30
max_top10_pct = 40
# Field WAJIB: kosong (null) -> skip. Kurangi daftar ini kalau sumber datamu tidak menyediakannya.
required_fields = market_cap, liquidity_usd, volume_h1, age_min, price_change_5m, top10_pct
# Field yang bernilai 0 dianggap data kosong:
zero_invalid_fields = market_cap, liquidity_usd, volume_h1, top10_pct

[security]
min_fees_sol = 0               # 0 = dimatikan (butuh GMGN berbayar)
min_lp_locked_pct = 80
max_sell_tax_pct = 10
max_rug_ratio = 0.3
require_authority_data = true  # mint/freeze/LP tak terbaca -> skip
max_transfer_fee_bps = 500     # ekstensi Token-2022 (butuh SOLANA_RPC_URL)
roundtrip_check = true         # kuotasi beli lalu jual; tak ada rute / rugi besar -> skip
max_roundtrip_loss_pct = 12

[entry]
size_sol = 0.5
max_open_positions = 5
max_slippage_bps = 300
daily_loss_limit_sol = 1.0
daily_profit_target_sol = 2.0
loss_streak_pause = 4
pause_minutes = 30
priority_fee_sol = 0.003       # per transaksi (prioritas + tip) - ASUMSI, sesuaikan
max_fee_per_tx_sol = 0.01

[exit]
poll_s = 3
stop_loss_pct = -30
take_profit_pct = 1000
trail_activate_pct = 10
trail_distance = 5             # mode points: poin profit; mode percent: % dari harga puncak
trail_mode = points
max_hold_min = 60
flat_band_pct = 3
flat_minutes = 10
price_source = jupiter         # jupiter | dexscreener

[dryrun]
jupiter_base = https://lite-api.jup.ag/swap/v1
jupiter_gap_s = 0.4
latency_haircut_pct = 1.0      # potongan harga fill akibat latensi (per sisi) - ASUMSI
tx_fail_rate = 0.05
seed = 0

[report]
dump_drawdown_pct = 30
dump_min_peak_pct = 20
min_sample = 30
"""


# ---------------- config
class Cfg:
    def __init__(self, path):
        self.p = configparser.ConfigParser(inline_comment_prefixes=("#", ";"), interpolation=None)
        self.p.read_string(DEFAULT_INI)
        if not path.exists():
            path.write_text(DEFAULT_INI)
            print("config.ini dibuat di %s (isinya placeholder). Periksa dulu." % path)
        self.p.read(str(path))

    def f(self, sec, key): return self.p.getfloat(sec, key)
    def i(self, sec, key): return int(self.p.getfloat(sec, key))
    def s(self, sec, key): return self.p.get(sec, key).strip()
    def b(self, sec, key): return self.p.getboolean(sec, key)
    def list(self, sec, key): return [x.strip() for x in self.p.get(sec, key).split(",") if x.strip()]


# ---------------- database
SCHEMA = """
CREATE TABLE IF NOT EXISTS tokens(addr TEXT PRIMARY KEY, symbol TEXT, name TEXT, sources TEXT, t0 REAL, price0 REAL,
  status TEXT, reason TEXT, metrics TEXT, security TEXT, decided REAL);
CREATE TABLE IF NOT EXISTS snaps(addr TEXT, t REAL, price REAL, liq REAL);
CREATE INDEX IF NOT EXISTS ix_snaps ON snaps(addr, t);
CREATE TABLE IF NOT EXISTS positions(id INTEGER PRIMARY KEY AUTOINCREMENT, addr TEXT, symbol TEXT, source TEXT,
  t_entry REAL, size_sol REAL, token_raw TEXT, entry_price REAL, status TEXT, t_exit REAL, exit_sol REAL,
  pnl_sol REAL, pnl_pct REAL, peak_pct REAL, reason TEXT, est INTEGER DEFAULT 0);
"""
_conn = []


def q(sql, args=()):
    with DBL:
        if not _conn:
            c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
            c.execute("PRAGMA journal_mode=WAL")
            c.executescript(SCHEMA)
            _conn.append(c)
        cur = _conn[0].execute(sql, args)
        rows = cur.fetchall()
        _conn[0].commit()
        return rows


def notify(msg):
    print(msg)
    if os.environ.get("TELEGRAM_BOT_TOKEN") and tb.owner():
        tb.send(tb.owner(), msg)


# ---------------- Jupiter (kuotasi untuk fill simulasi)
def quote(inp, out, amount, cfg):
    base, key = cfg.s("dryrun", "jupiter_base"), os.environ.get("JUPITER_API_KEY")
    if key and "lite-api" in base:
        base = "https://api.jup.ag/swap/v1"
    if tb._hosts.get(urllib.parse.urlparse(base).netloc, 0) > time.time() + 5:
        return None                                    # host Jupiter sedang dijeda (429): jangan memblokir monitor exit
    url = "%s/quote?%s" % (base, urllib.parse.urlencode(dict(
        inputMint=inp, outputMint=out, amount=int(amount), slippageBps=cfg.i("entry", "max_slippage_bps"))))
    d = tb.get(url, gap=cfg.f("dryrun", "jupiter_gap_s"), headers={"x-api-key": key} if key else None)
    try:
        return int(d["outAmount"])
    except Exception:
        return None


def sell_value_sol(addr, raw, cfg):
    out = quote(addr, WSOL, raw, cfg)
    return out / 1e9 if out else None


# ---------------- RPC (trigger migrasi + cek Token-2022)
def rpc(method, params, timeout=15):
    url = os.environ.get("SOLANA_RPC_URL")
    if not url:
        return None
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    try:
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", "User-Agent": "engine/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r).get("result")
    except Exception as e:
        print("rpc %s error: %s" % (method, type(e).__name__))
        return None


def token2022_reasons(addr, cfg):
    res = rpc("getAccountInfo", [addr, {"encoding": "jsonParsed"}])
    val = (res or {}).get("value") or {}
    info = ((val.get("data") or {}).get("parsed") or {}).get("info") or {}
    bad, out = {"transferHook": "transfer hook", "permanentDelegate": "permanent delegate",
                "nonTransferable": "non-transferable"}, []
    for e in info.get("extensions") or []:
        name, st = e.get("extension"), e.get("state") or {}
        if name in bad:
            out.append("Token-2022: " + bad[name])
        elif name == "defaultAccountState" and str(st.get("accountState")).lower() == "frozen":
            out.append("Token-2022: default akun frozen")
        elif name == "transferFeeConfig":
            bps = max([num((st.get(k) or {}).get("transferFeeBasisPoints")) or 0 for k in ("newerTransferFee", "olderTransferFee")])
            if bps > cfg.f("security", "max_transfer_fee_bps"):
                out.append("Token-2022: transfer fee %d bps" % bps)
    return out


def roundtrip_reasons(addr, cfg):
    lam = int(cfg.f("entry", "size_sol") * 1e9)
    raw = quote(WSOL, addr, lam, cfg)
    if not raw:
        return ["tidak ada rute beli"]
    back = quote(addr, WSOL, raw, cfg)
    if not back:
        return ["tidak ada rute jual (curiga honeypot)"]
    loss = (1 - back / lam) * 100
    return ["rugi putaran %.1f%% > %g%%" % (loss, cfg.f("security", "max_roundtrip_loss_pct"))] \
        if loss > cfg.f("security", "max_roundtrip_loss_pct") else []


# ---------------- filter (semua ambang dari config) + gate data kosong
RULES = [("market_cap", "min_market_cap", "max_market_cap"), ("liquidity_usd", "min_liquidity_usd", None),
         ("volume_h1", "min_volume_h1", None), ("age_min", "min_age_min", "max_age_min"),
         ("price_change_1m", "min_price_change_1m", None), ("price_change_5m", "min_price_change_5m", "max_price_change_5m"),
         ("top10_pct", None, "max_top10_pct"), ("dev_holding_pct", None, "max_dev_holding_pct"),
         ("bundler_pct", None, "max_bundler_pct"), ("sniper_pct", None, "max_sniper_pct")]
DEX_FIELDS = ("market_cap", "liquidity_usd", "volume_h1", "age_min", "price_change_1m", "price_change_5m")
SEC_FIELDS = ("top10_pct", "dev_holding_pct", "bundler_pct", "sniper_pct")


def apply_filter(m, cfg, fields):
    req, zero, out = cfg.list("filter", "required_fields"), cfg.list("filter", "zero_invalid_fields"), []
    for f, lo, hi in RULES:
        if f not in fields:
            continue
        v = m.get(f)
        if v is None or (v == 0 and f in zero):
            if f in req:
                out.append("data kosong: " + f)
            continue
        if lo and v < cfg.f("filter", lo):
            out.append("%s %.4g < %s" % (f, v, lo))
        if hi and v > cfg.f("filter", hi):
            out.append("%s %.4g > %s" % (f, v, hi))
    return out


def change_1m(addr):
    rows = q("SELECT t, price FROM snaps WHERE addr=? ORDER BY t DESC LIMIT 60", (addr,))
    if not rows:
        return None
    t_now, p_now = rows[0]
    for t, pr in rows:
        if t_now - t >= 55 and pr:
            return (p_now / pr - 1) * 100
    return None


def dex_metrics(addr, p):
    created, pc = p.get("pairCreatedAt") or 0, p.get("priceChange") or {}
    return dict(market_cap=num(p.get("marketCap") or p.get("fdv")), liquidity_usd=num((p.get("liquidity") or {}).get("usd")),
                volume_h1=num((p.get("volume") or {}).get("h1")),
                age_min=(time.time() * 1000 - created) / 60000 if created else None,
                price_change_5m=num(pc.get("m5")), price_change_1m=change_1m(addr),
                narrative=tb.themes_of("%s %s %s" % ((p.get("baseToken") or {}).get("name", ""),
                                                      (p.get("baseToken") or {}).get("symbol", ""), tb.DESC.get(addr, ""))))


def security_stage(addr, p, cfg):
    """Return (alasan_gagal, rows_teks, metrik_angka). Keputusan memakai ambang config; teks hanya informasi."""
    tcfg = dict(tb.CFG, min_fees_sol=cfg.f("security", "min_fees_sol"))
    rows, m = tb.analyze_full(addr, p, tcfg)
    pct = lambda x: None if x is None else x * 100
    m.update(top10_pct=pct(m.get("top10")), dev_holding_pct=pct(m.get("dev")), sniper_pct=pct(m.get("sniper")),
             bundler_pct=pct(m.get("bundler")))
    r, mf = [], cfg.f("security", "min_fees_sol")
    if mf > 0:
        fee = m.get("fees_sol")
        r.append("fees tak terbaca") if fee is None else (r.append("fees %.1f < %g SOL" % (fee, mf)) if fee < mf else None)
    if m.get("honeypot"):
        r.append("honeypot")
    st = m.get("sell_tax")
    if st is not None and st * 100 > cfg.f("security", "max_sell_tax_pct"):
        r.append("pajak jual tinggi")
    need = cfg.b("security", "require_authority_data")
    for k, lab in (("mint_on", "mint authority"), ("freeze_on", "freeze authority")):
        v = m.get(k)
        if v:
            r.append(lab + " aktif")
        elif v is None and need:
            r.append(lab + " tak terbaca")
    lp = m.get("lp_locked_pct")
    if lp is None:
        if need:
            r.append("LP tak terbaca")
    elif lp < cfg.f("security", "min_lp_locked_pct"):
        r.append("LP terkunci %.0f%% < %g%%" % (lp, cfg.f("security", "min_lp_locked_pct")))
    rr = m.get("rug_ratio")
    if rr is not None and rr > cfg.f("security", "max_rug_ratio"):
        r.append("rug_ratio %.2f tinggi" % rr)
    r += token2022_reasons(addr, cfg)
    if cfg.b("security", "roundtrip_check"):
        r += roundtrip_reasons(addr, cfg)
    return r, rows, m


# ---------------- aturan exit (dipakai bersama oleh dry run DAN replay)
def exit_rules(cfg):
    g = lambda k: cfg.f("exit", k)
    return dict(stop_loss=g("stop_loss_pct") / 100, take_profit=g("take_profit_pct") / 100, trail_act=g("trail_activate_pct") / 100,
                trail_dist=g("trail_distance") / 100, trail_mode=cfg.s("exit", "trail_mode"), max_hold_s=g("max_hold_min") * 60,
                flat_band=g("flat_band_pct") / 100, flat_s=g("flat_minutes") * 60)


class ExitState:
    def __init__(self, rules, peak=0.0):
        self.r, self.peak, self.samples = rules, peak, []
        self.trail_on = peak >= rules["trail_act"]

    def update(self, pnl, t, t_entry):
        r = self.r
        self.peak = max(self.peak, pnl)
        self.samples = [(tt, p) for tt, p in self.samples if tt >= t - r["flat_s"] - 1] + [(t, pnl)]
        if pnl <= r["stop_loss"]:
            return "stop_loss"                      # exit di nilai yang TERAMATI (termasuk gap), bukan di level stop
        if pnl >= r["take_profit"]:
            return "take_profit"
        if self.peak >= r["trail_act"]:
            self.trail_on = True
        if self.trail_on:
            lvl = self.peak - r["trail_dist"] if r["trail_mode"] == "points" else (1 + self.peak) * (1 - r["trail_dist"]) - 1
            if pnl <= lvl:
                return "trailing"
        if t - t_entry >= r["max_hold_s"]:
            return "max_hold"
        if t - t_entry >= r["flat_s"]:
            win = [p for tt, p in self.samples if tt >= t - r["flat_s"]]
            if win and max(win) - min(win) <= 2 * r["flat_band"]:
                return "datar"
        return None


# ---------------- entry & exit dry run
def today_pnl():
    mid = time.mktime(time.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d"))
    return q("SELECT COALESCE(SUM(pnl_sol),0) FROM positions WHERE status='closed' AND t_exit>=?", (mid,))[0][0]


def entry_block(cfg):
    if Path("STOP").exists():
        return "file STOP ada"
    if RT["paused"]:
        return "di-pause"
    if time.time() < RT["pause_until"]:
        return "jeda setelah rugi beruntun"
    if q("SELECT COUNT(*) FROM positions WHERE status='open'")[0][0] >= cfg.i("entry", "max_open_positions"):
        return "posisi penuh"
    pnl = today_pnl()
    if pnl <= -cfg.f("entry", "daily_loss_limit_sol"):
        return "batas rugi harian tercapai"
    if pnl >= cfg.f("entry", "daily_profit_target_sol"):
        return "target profit harian tercapai (dikunci)"
    if cfg.f("entry", "priority_fee_sol") > cfg.f("entry", "max_fee_per_tx_sol"):
        return "fee per transaksi melebihi batas"
    return None


def try_entry(addr, sym, source, p, cfg):
    with ENTRY_LOCK:
        why = entry_block(cfg)
        if why:
            q("UPDATE tokens SET reason=? WHERE addr=?", ("lolos tapi tak masuk: " + why, addr))
            return None
        if random.random() < cfg.f("dryrun", "tx_fail_rate"):
            q("UPDATE tokens SET reason=? WHERE addr=?", ("transaksi beli gagal (simulasi)", addr))
            return None
        size = cfg.f("entry", "size_sol")
        raw = quote(WSOL, addr, int(size * 1e9), cfg)
        if not raw:
            q("UPDATE tokens SET reason=? WHERE addr=?", ("tak ada kuotasi beli", addr))
            return None
        raw = int(raw * (1 - cfg.f("dryrun", "latency_haircut_pct") / 100))
        q("INSERT INTO positions(addr,symbol,source,t_entry,size_sol,token_raw,entry_price,status,peak_pct) "
          "VALUES (?,?,?,?,?,?,?,?,0)", (addr, sym, source, time.time(), size, str(raw), float(p.get("priceUsd") or 0), "open"))
    notify("[DRY ENTRY] %s %.2f SOL (sumber %s)\n%s" % (sym, size, source, p.get("url") or ""))
    return True


def position_value(addr, raw, size, entry_price, cfg):
    if cfg.s("exit", "price_source") == "jupiter":
        v = sell_value_sol(addr, int(raw), cfg)
        if v:
            return v, 0
    r = q("SELECT price FROM snaps WHERE addr=? ORDER BY t DESC LIMIT 1", (addr,))
    if r and r[0][0] and entry_price:
        return size * r[0][0] / entry_price, 1            # perkiraan tanpa price impact
    return None, 0


def close_position(pid, sym, size, val, reason, est, cfg):
    exit_sol = val * (1 - cfg.f("dryrun", "latency_haircut_pct") / 100)
    pnl_sol = exit_sol - size - 2 * cfg.f("entry", "priority_fee_sol")
    q("UPDATE positions SET status='closed', t_exit=?, exit_sol=?, pnl_sol=?, pnl_pct=?, reason=?, est=? WHERE id=?",
      (time.time(), exit_sol, pnl_sol, pnl_sol / size * 100, reason, est, pid))
    RT["streak"] = RT["streak"] + 1 if pnl_sol < 0 else 0
    if RT["streak"] >= cfg.i("entry", "loss_streak_pause"):
        RT["pause_until"], RT["streak"] = time.time() + cfg.f("entry", "pause_minutes") * 60, 0
        notify("Jeda entry %g menit setelah rugi beruntun." % cfg.f("entry", "pause_minutes"))
    notify("[DRY EXIT] %s %s pnl %+.1f%% (%+.3f SOL)%s" % (sym, reason, pnl_sol / size * 100, pnl_sol, " ~perkiraan" if est else ""))


def exit_tick(cfg, states, rules):
    for pid, addr, sym, t_entry, size, raw, ep, peak in q(
            "SELECT id, addr, symbol, t_entry, size_sol, token_raw, entry_price, peak_pct FROM positions WHERE status='open'"):
        val, est = position_value(addr, raw, size, ep, cfg)
        if val is None:
            continue
        pnl = val / size - 1
        st = states.setdefault(pid, ExitState(rules, (peak or 0) / 100))
        reason = st.update(pnl, time.time(), t_entry)
        q("UPDATE positions SET peak_pct=? WHERE id=?", (st.peak * 100, pid))
        if reason and random.random() >= cfg.f("dryrun", "tx_fail_rate"):      # exit gagal -> coba lagi di poll berikutnya
            close_position(pid, sym, size, val, reason, est, cfg)


def exit_loop(cfg):
    states, rules = {}, exit_rules(cfg)
    while not STOP.is_set():
        try:
            exit_tick(cfg, states, rules)
        except Exception as e:
            print("exit error:", e)
        STOP.wait(cfg.f("exit", "poll_s"))


# ---------------- pipeline per token
def save_snap(addr, p):
    pr = num(p.get("priceUsd"))
    if pr:
        q("INSERT INTO snaps VALUES (?,?,?,?)", (addr, time.time(), pr, num((p.get("liquidity") or {}).get("usd"))))


def wait_pair(addr, cfg):
    end = time.time() + cfg.f("monitor", "pair_wait_s")
    while time.time() < end and not STOP.is_set():
        p = tb.lookup([addr], "solana").get(addr)
        if p and (num(p.get("priceUsd")) or 0) > 0:
            return p
        STOP.wait(4)
    return None


def decide(addr, status, reasons, metrics=None, rows=None):
    q("UPDATE tokens SET status=?, reason=?, metrics=?, security=?, decided=? WHERE addr=?",
      (status, "; ".join(reasons), json.dumps(metrics or {}, default=str),
       json.dumps([[a, b, c] for a, b, c in (rows or [])]), time.time(), addr))


def process(addr, source, cfg):
    row = q("SELECT sources FROM tokens WHERE addr=?", (addr,))
    if row:
        srcs = row[0][0].split(",")
        if source not in srcs:
            q("UPDATE tokens SET sources=? WHERE addr=?", (",".join(srcs + [source]), addr))
        return
    q("INSERT INTO tokens(addr,sources,t0,status) VALUES (?,?,?,'new')", (addr, source, time.time()))
    p = wait_pair(addr, cfg)
    if not p:
        return decide(addr, "rejected", ["pair tidak ditemukan"])
    bt = p["baseToken"]
    q("UPDATE tokens SET symbol=?, name=?, price0=?, status='watching' WHERE addr=?",
      (bt.get("symbol", "?"), bt.get("name", ""), num(p.get("priceUsd")), addr))
    save_snap(addr, p)
    STOP.wait(cfg.f("monitor", "warmup_s"))
    p = tb.lookup([addr], "solana").get(addr) or p
    m = dex_metrics(addr, p)
    why = apply_filter(m, cfg, DEX_FIELDS)
    if why:
        return decide(addr, "rejected", why, m)
    sec, rows, sm = security_stage(addr, p, cfg)
    m.update({k: sm.get(k) for k in SEC_FIELDS})
    why = apply_filter(m, cfg, SEC_FIELDS) + sec
    if why:
        return decide(addr, "rejected", why, m, rows)
    decide(addr, "passed", [], m, rows)
    try_entry(addr, bt.get("symbol", "?"), source, p, cfg)


def safe_process(addr, source, cfg):
    try:
        process(addr, source, cfg)
    except Exception as e:
        print("pipeline error %s: %s" % (addr[:6], e))


def dispatcher(cfg):
    ex = ThreadPoolExecutor(max_workers=cfg.i("monitor", "pipeline_workers"))
    while not STOP.is_set():
        try:
            addr, src = EVENTS.get(timeout=1)
        except queue.Empty:
            continue
        ex.submit(safe_process, addr, src, cfg)


# ---------------- monitor: snapshot harga semua token yang dilacak
def monitor_tick(cfg, due):
    now, track = time.time(), cfg.f("monitor", "track_minutes") * 60
    fast, mid, slow = cfg.f("monitor", "snap_fast_s"), cfg.f("monitor", "snap_mid_s"), cfg.f("monitor", "snap_slow_s")
    batch = []
    for addr, t0 in q("SELECT addr, t0 FROM tokens WHERE price0 IS NOT NULL AND t0>?", (now - track,)):
        if due.get(addr, 0) <= now:
            age = now - t0
            due[addr] = now + (fast if age < 600 else mid if age < 3600 else slow)
            batch.append(addr)
    for i in range(0, len(batch), 30):
        for a, p in tb.lookup(batch[i:i + 30], "solana").items():
            save_snap(a, p)


def monitor_loop(cfg):
    due = {}
    while not STOP.is_set():
        try:
            monitor_tick(cfg, due)
        except Exception as e:
            print("monitor error:", e)
        STOP.wait(1.0)


# ---------------- trigger
def gt_tokens(kind):
    d = tb.get("%s/networks/solana/%s" % (tb.GT, kind), gap=2.2) or {}
    out = []
    for p in d.get("data", []):
        try:
            out.append(p["relationships"]["base_token"]["data"]["id"].split("_", 1)[1])
        except Exception:
            pass
    return out


def run_loop(fn, interval):
    while not STOP.is_set():
        try:
            fn()
        except Exception as e:
            print("trigger error:", e)
        STOP.wait(interval)


def migration_mints(sig):
    tx = rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
    meta = (tx or {}).get("meta") or {}
    if not any("migrate" in l.lower() for l in meta.get("logMessages") or []):
        return []
    return list(dict.fromkeys(b["mint"] for b in meta.get("postTokenBalances") or []
                              if b.get("mint") and b["mint"] != WSOL))[:1]


def pump_loop(cfg):
    if not os.environ.get("SOLANA_RPC_URL"):
        print("pump_migration aktif tapi SOLANA_RPC_URL kosong -> trigger ini dinonaktifkan")
        return
    acct, last = cfg.s("trigger", "pump_migration_account"), None
    while not STOP.is_set():
        opts = {"limit": 20}
        if last:
            opts["until"] = last
        sigs = rpc("getSignaturesForAddress", [acct, opts]) or []
        if sigs:
            if last is not None:                       # putaran pertama hanya menandai posisi, tidak memproses riwayat
                for s in reversed(sigs):
                    if s.get("err") is None:
                        for mint in migration_mints(s["signature"]):
                            EVENTS.put((mint, "pump_migration"))
            last = sigs[0]["signature"]
        STOP.wait(cfg.f("trigger", "pump_poll_s"))


# ---------------- statistik & replay
def load_paths(group="all", min_snaps=8):
    cond = {"passed": " AND status='passed'", "rejected": " AND status='rejected'", "all": ""}[group]
    out = []
    for addr, src, status, t0, dec, met in q("SELECT addr, sources, status, t0, decided, metrics FROM tokens WHERE price0 IS NOT NULL" + cond):
        snaps = q("SELECT t, price FROM snaps WHERE addr=? AND price>0 ORDER BY t", (addr,))
        if len(snaps) >= min_snaps:
            try:
                themes = json.loads(met or "{}").get("narrative") or []
            except Exception:
                themes = []
            out.append(dict(addr=addr, src=(src or "?").split(",")[0], status=status, t0=t0, decided=dec, path=snaps, themes=themes))
    return out


def hit_before(path, up, down):
    p0 = path[0][1]
    for _, p in path[1:]:
        r = p / p0 - 1
        if r >= up / 100:
            return True
        if r <= -down / 100:
            return False
    return False


def peak_dump(path, dd, minpeak):
    p0, peak, tpeak, tdump = path[0][1], path[0][1], path[0][0], None
    for t, p in path[1:]:
        if p > peak:
            peak, tpeak = p, t
        if tdump is None and peak / p0 - 1 >= minpeak / 100 and p <= peak * (1 - dd / 100):
            tdump = t
    return (tpeak - path[0][0]) / 60, (peak / p0 - 1) * 100, (None if tdump is None else (tdump - path[0][0]) / 60)


def med(v):
    v = [x for x in v if x is not None]
    return statistics.median(v) if v else None


def group_line(label, items, cfg):
    n, ms = len(items), cfg.i("report", "min_sample")
    dd, mp = cfg.f("report", "dump_drawdown_pct"), cfg.f("report", "dump_min_peak_pct")
    pk = [peak_dump(i["path"], dd, mp) for i in items]
    out = ["%s: n=%d%s" % (label, n, "  (SAMPEL KECIL, jangan disimpulkan)" if n < ms else "")]
    for s in (30, 50):
        out.append("  menyentuh +T%% sebelum -%d%%: " % s + ", ".join(
            "+%d%%: %.0f%%" % (t, 100 * sum(hit_before(i["path"], t, s) for i in items) / n) for t in (50, 100, 300, 1000)))
    f = lambda x: "-" if x is None else "%.1f" % x
    out.append("  median gain puncak %s%% | menit ke puncak %s | menit ke dump %s (dump terukur pada %d token)" % (
        f(med([x[1] for x in pk])), f(med([x[0] for x in pk])), f(med([x[2] for x in pk])), sum(1 for x in pk if x[2] is not None)))
    return out


def bootstrap_ci(vals, n=2000):
    if len(vals) < 5:
        return None
    ms = sorted(statistics.mean(random.choices(vals, k=len(vals))) for _ in range(n))
    return ms[int(0.025 * n)], ms[int(0.975 * n)]


def pnl_summary(vals):
    """vals: pnl per posisi (fraksi). Mengembalikan teks ringkas (win rate, median, expectancy + CI, porsi top-5)."""
    if not vals:
        return "belum ada posisi tertutup"
    tot, top5 = sum(vals), sum(sorted(vals, reverse=True)[:5])
    ci = bootstrap_ci(vals)
    return "n=%d win %.0f%% | median %+.1f%% | expectancy %+.1f%%%s | porsi profit top-5: %s" % (
        len(vals), 100 * sum(v > 0 for v in vals) / len(vals), statistics.median(vals) * 100, statistics.mean(vals) * 100,
        " (CI95 %+.1f%%..%+.1f%%)" % (ci[0] * 100, ci[1] * 100) if ci else " (CI: data kurang)",
        "%.0f%%" % (100 * top5 / tot) if tot > 0 else "-")


def report_text(cfg, short=False):
    tok = q("SELECT status, COUNT(*) FROM tokens GROUP BY status")
    out = ["Token: " + ", ".join("%s=%d" % (s, n) for s, n in tok) if tok else "Belum ada token tercatat."]
    items = load_paths("all")
    by = {}
    for i in items:
        by.setdefault(i["src"], []).append(i)
    for src, its in sorted(by.items()):
        out += group_line("sumber %s" % src, its, cfg)[:2 if short else None]
    if not short:
        byth = {}
        for i in items:
            for t in i["themes"]:
                byth.setdefault(t, []).append(i)
        for t, its in sorted(byth.items(), key=lambda kv: -len(kv[1])):
            out += group_line("narasi %s" % t, its, cfg)[:2]
        for st in ("passed", "rejected"):
            its = [i for i in items if i["status"] == st]
            if its:
                out += group_line("status %s (rejected = hasil kontrafaktual)" % st, its, cfg)
    closed = [r[0] / 100 for r in q("SELECT pnl_pct FROM positions WHERE status='closed'")]
    out.append("Dry run (bersih biaya): " + pnl_summary(closed))
    opn = q("SELECT COUNT(*) FROM positions WHERE status='open'")[0][0]
    out.append("Posisi terbuka: %d | PnL hari ini: %+.3f SOL" % (opn, today_pnl()))
    return "\n".join(out)


def simulate_path(path, rules):
    t0, p0 = path[0]
    st, last = ExitState(rules), (t0, 0.0)
    for t, p in path[1:]:
        pnl = p / p0 - 1
        reason = st.update(pnl, t, t0)
        last = (t, pnl)
        if reason:
            return pnl, reason, t - t0
    return last[1], "data_habis", last[0] - t0


def replay(cfg, sets, group):
    items = load_paths(group)
    for it in items:                                   # masuk pada saat keputusan; kalau tak ada, pada snapshot pertama
        start = it["decided"] or it["t0"]
        later = [x for x in it["path"] if x[0] >= start]
        it["path"] = later if len(later) >= 5 else it["path"]
    if not items:
        return "Belum ada data rekaman untuk grup '%s'." % group
    grid = [(k, v.split(",")) for k, v in (s.split("=", 1) for s in sets)]
    combos = list(itertools.product(*[[(k, x) for x in vs] for k, vs in grid])) or [()]
    cost = 2 * cfg.f("dryrun", "latency_haircut_pct") / 100 + 2 * cfg.f("entry", "priority_fee_sol") / cfg.f("entry", "size_sol")
    out = ["Replay %d token (grup %s). Biaya putaran dikurangkan: %.1f%% (perkiraan; harga tengah, tanpa price impact)." % (
        len(items), group, cost * 100)]
    for combo in combos:
        c2 = Cfg(CFG_PATH)
        for k, v in combo:
            sec = "exit" if k in dict(c2.p.items("exit")) else None
            assert sec, "kunci exit tidak dikenal: " + k
            c2.p.set("exit", k, v)
        rules, res = exit_rules(c2), []
        for it in items:
            pnl, reason, held = simulate_path(it["path"], rules)
            res.append((pnl - cost, reason, held))
        reasons = {}
        for _, r, _ in res:
            reasons[r] = reasons.get(r, 0) + 1
        out.append("[%s] %s | exit: %s | rata2 tahan %.0f mnt" % (
            ", ".join("%s=%s" % kv for kv in combo) or "config saat ini", pnl_summary([r[0] for r in res]), reasons,
            statistics.mean(r[2] for r in res) / 60))
    return "\n".join(out)


# ---------------- perintah Telegram (khusus owner, lewat hook trenches_bot.EXTRA_CMDS)
def dry_cmd(arg):
    rows = q("SELECT symbol, peak_pct, t_entry FROM positions WHERE status='open'")
    lines = ["%s (puncak %+.0f%%, %d mnt)" % (s, pk or 0, (time.time() - t) / 60) for s, pk, t in rows] or ["tidak ada"]
    return "Posisi dry run terbuka:\n" + "\n".join(lines) + "\nPnL hari ini: %+.3f SOL%s" % (
        today_pnl(), "\nENTRY DI-PAUSE" if RT["paused"] else "")


def register_commands():
    tb.EXTRA_CMDS.update({"/dry": dry_cmd, "/stats": lambda a: report_text(CFGOBJ[0], short=True),
                          "/pause": lambda a: RT.update(paused=True) or "Entry di-pause.",
                          "/resume": lambda a: RT.update(paused=False) or "Entry dilanjutkan."})
    tb.ADMIN_HELP += "\n/dry - posisi dry run\n/stats - statistik monitor\n/pause /resume - jeda entry"


# ---------------- run
def run_engine(cfg, with_alerts):
    CFGOBJ[0] = cfg
    random.seed(cfg.i("dryrun", "seed") or None)
    os.environ["REQUIRE_FEES"] = "0"                    # keputusan memakai config, bukan status teks analyze
    q("SELECT 1")
    print("ENGINE DRY RUN (tanpa transaksi asli) | config: %s | %s" % (CFG_PATH, tb.tools_status()))
    if not os.environ.get("SOLANA_RPC_URL"):
        print("Catatan: SOLANA_RPC_URL kosong -> trigger migrasi dan cek Token-2022 tidak aktif.")
    threads = [dispatcher, monitor_loop, exit_loop]
    th = [threading.Thread(target=f, args=(cfg,), daemon=True) for f in threads]
    if cfg.b("trigger", "gt_trending"):
        th.append(threading.Thread(target=run_loop, daemon=True, args=(
            lambda: [EVENTS.put((a, "gt_trending")) for a in gt_tokens("trending_pools")], cfg.f("trigger", "gt_trending_poll_s"))))
    if cfg.b("trigger", "gt_new_pools"):
        th.append(threading.Thread(target=run_loop, daemon=True, args=(
            lambda: [EVENTS.put((a, "gt_new")) for a in gt_tokens("new_pools")], cfg.f("trigger", "gt_trending_poll_s"))))
    if cfg.b("trigger", "dex_boost"):
        th.append(threading.Thread(target=run_loop, daemon=True, args=(
            lambda: [EVENTS.put((a, "dex_boost")) for a in tb.profile_tokens("solana")], 30)))
    if cfg.b("trigger", "pump_migration"):
        th.append(threading.Thread(target=pump_loop, args=(cfg,), daemon=True))
    if os.environ.get("TELEGRAM_BOT_TOKEN") and tb.owner():
        register_commands()
        th.append(threading.Thread(target=tb.listener, daemon=True))
        tb.send(tb.owner(), "Engine DRY RUN aktif. /dry /stats /pause /resume")
    if with_alerts:
        th.append(threading.Thread(target=run_loop, daemon=True, args=(lambda: tb.cycle(tb.CFG), tb.CFG["poll"])))
    for t in th:
        t.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nBerhenti.")
        STOP.set()


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")
    r = sub.add_parser("run")
    r.add_argument("--with-alerts", action="store_true")
    sub.add_parser("report")
    sub.add_parser("init")
    rp = sub.add_parser("replay")
    rp.add_argument("--set", action="append", default=[])
    rp.add_argument("--group", default="passed", choices=["passed", "rejected", "all"])
    a = ap.parse_args()
    cfg = Cfg(CFG_PATH)
    CFGOBJ[0] = cfg
    cmd = a.cmd or "run"
    if cmd == "init":
        print("config.ini siap di", CFG_PATH)
    elif cmd == "report":
        print(report_text(cfg))
    elif cmd == "replay":
        print(replay(cfg, a.set, a.group))
    else:
        run_engine(cfg, getattr(a, "with_alerts", False))


if __name__ == "__main__":
    main()
