import asyncio, html, io, json, logging, math, os, re, shutil, sqlite3, time
from datetime import datetime

import aiohttp, aiosqlite
from aiogram import Bot, Dispatcher, F, Router, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (BufferedInputFile, CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, KeyboardButton, Message,
                           ReplyKeyboardMarkup)
from dotenv import load_dotenv

load_dotenv()
load_dotenv('env.txt')   # اگه فایل .env مخفی بود، اسمش رو env.txt بذار
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("shop")

USER_TOKEN = os.environ["USER_BOT_TOKEN"]
ADMIN_TOKEN = os.environ["ADMIN_BOT_TOKEN"]
ADMIN_IDS = [int(x) for x in os.environ["ADMIN_IDS"].split(",") if x.strip()]
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
CARD_NUMBER = os.environ["CARD_NUMBER"]
CARD_OWNER = os.environ["CARD_OWNER"]
DB_PATH = os.getenv("DB_PATH", "data/shop.db")
FRAGMENT_HASH = os.getenv("FRAGMENT_HASH", "").strip()
FRAGMENT_COOKIE = os.getenv("FRAGMENT_COOKIE", "").strip()
FRAGMENT_WALLET = os.getenv("FRAGMENT_WALLET", "").strip()          # آدرس ولت خودت (فقط برای استعلام قیمت)
FRAGMENT_STARS_USER = os.getenv("FRAGMENT_STARS_USER", "").strip()  # یوزرنیم خودت
FRAGMENT_PREMIUM_USER = os.getenv("FRAGMENT_PREMIUM_USER", "").strip()  # یه اکانت بدون پرمیوم
NANO = 10 ** 9

prop = DefaultBotProperties(parse_mode="HTML")
user_bot = Bot(USER_TOKEN, default=prop)
admin_bot = Bot(ADMIN_TOKEN, default=prop)

# ───────────────────────── DB ─────────────────────────
db: aiosqlite.Connection = None

DEFAULTS = {
    "star_gift50_usd": "1.0",      # قیمت خرید گیفت ۵۰تایی از فراگمنت (دلار) - با /set تنظیم کن
    "premium_usd_3": "12.0", "premium_usd_6": "16.5", "premium_usd_12": "29.0",
    "star_margin": "3.0",          # درصد سود استارز (برای ۲۵۰ به بالا)
    "star_free_below": "250",      # زیر این تعداد سود صفر
    "star_min": "50",
    "premium_margin": "3.0",
    "ton_fee_pct_lo": "1.5",       # سود (داخل کارمزد) تا ton_mid_threshold
    "ton_fee_pct_mid": "2.0",      # بالاتر از ton_mid_threshold
    "ton_fee_pct_hi": "2.5",       # بالاتر از ton_hi_threshold
    "ton_mid_threshold": "1.5", "ton_hi_threshold": "10",
    "ton_net_fee_ton": "0.01",     # کارمزد واقعی شبکه برای هر انتقال تون (TON) - هم خرید هم برداشت
    "ton_min": "0.5",
    "wd_min_ton": "0.5",
    "kyc_limit": "400000",         # سقف مجموع خرید بدون احراز هویت (تومان)
    "usdt_toman_manual": "0",      # اگر >0 باشه به جای نوبیتکس استفاده میشه
    "order_ttl_min": "15",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, username TEXT, phone TEXT,
  banned INTEGER DEFAULT 0, kyc INTEGER DEFAULT 0, ton_nano INTEGER DEFAULT 0, created REAL);
CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, kind TEXT,
  title TEXT, details TEXT, toman INTEGER, ton_nano INTEGER DEFAULT 0, status TEXT,
  receipt_uid TEXT, tracking TEXT, created REAL, expires REAL);
CREATE TABLE IF NOT EXISTS withdrawals(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
  nano INTEGER, fee_nano INTEGER, address TEXT, memo TEXT, status TEXT, created REAL);
CREATE TABLE IF NOT EXISTS kyc(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, card TEXT,
  status TEXT, created REAL);
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS support(chat_id INTEGER, msg_id INTEGER, user_id INTEGER, PRIMARY KEY(chat_id,msg_id));
CREATE UNIQUE INDEX IF NOT EXISTS ux_track ON orders(tracking) WHERE tracking IS NOT NULL AND status IN ('pending','approved');
"""

async def init_db():
    global db
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.executescript(SCHEMA)
    for k, v in DEFAULTS.items():
        await db.execute("INSERT OR IGNORE INTO settings(k,v) VALUES(?,?)", (k, v))
    await db.commit()

async def execute(sql, args=()):
    async with db.execute(sql, args) as cur:
        res = (cur.lastrowid, cur.rowcount)
    await db.commit()
    return res

async def fetchone(sql, args=()):
    async with db.execute(sql, args) as cur:
        return await cur.fetchone()

async def fetchall(sql, args=()):
    async with db.execute(sql, args) as cur:
        return await cur.fetchall()

async def sget(k):
    r = await fetchone("SELECT v FROM settings WHERE k=?", (k,))
    return r["v"] if r else DEFAULTS[k]

async def sf(k):
    return float(await sget(k))

# ───────────────────────── helpers ─────────────────────────
def fm(n): return f"{int(n):,}"
def esc(s): return html.escape(str(s))
def now(): return time.time()
def round_to(x, step=1000): return int(round(x / step) * step)
_DIG = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
def norm(s): return s.translate(_DIG).replace(",", "").replace("٬", "").strip()
def num(s):
    try:
        v = float(norm(s)); return v if v > 0 else None
    except Exception:
        return None
def ts_fmt(t): return datetime.fromtimestamp(t).strftime("%Y/%m/%d %H:%M")

# ───────────────────────── prices ─────────────────────────
P = {"ton_usdt": 0.0, "usdt_toman": 0.0, "ts": 0.0}

async def price_loop():
    while True:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
                async with s.get("https://api.binance.com/api/v3/ticker/price?symbol=TONUSDT") as r:
                    P["ton_usdt"] = float((await r.json())["price"])
                try:
                    async with s.get("https://api.nobitex.ir/v3/orderbook/USDTIRT") as r:
                        P["usdt_toman"] = float((await r.json())["lastTradePrice"]) / 10
                except Exception as e:
                    log.warning("nobitex failed: %s", e)
                P["ts"] = time.time()
                log.info("prices: %s", P)
        except Exception as e:
            log.warning("price update failed: %s", e)
        await asyncio.sleep(180)


# ── قیمت خرید خودکار از فراگمنت (بدون API رسمی؛ اسکرپ) ──
FRAG_UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
           "X-Requested-With": "XMLHttpRequest"}
FRAG = {"ts": 0.0, "alerted": False, "last": "هنوز اجرا نشده"}
TAG = re.compile(r"<[^>]+>")

def _flat(txt):
    try:
        out = []
        def walk(x):
            if isinstance(x, dict): [walk(v) for v in x.values()]
            elif isinstance(x, list): [walk(v) for v in x]
            elif isinstance(x, str): out.append(x)
        walk(json.loads(txt)); txt = " ".join(out)
    except Exception:
        pass
    return html.unescape(TAG.sub(" ", txt))

async def _frag_stars_ton(s):
    async with s.get("https://fragment.com/stars/buy", headers=FRAG_UA) as r:
        page = await r.text()
    m = re.search(r'apiUrl"\s*:\s*"([^"]+)"', page)
    if not m: raise ValueError("apiUrl پیدا نشد")
    url = "https://fragment.com" + m.group(1).replace("\\/", "/")
    async with s.post(url, data={"mode": "new", "quantity": "50", "method": "updateStarsPrices"}, headers=FRAG_UA) as r:
        txt = _flat(await r.text())
    m = re.search(r"([\d]+(?:[.,]\d+)?)\s*TON", txt)
    if not m: raise ValueError("قیمت TON در پاسخ نبود: " + txt[:150])
    return float(m.group(1).replace(",", "."))

async def _frag_premium_ton(s):
    async with s.get("https://fragment.com/premium/gift", headers=FRAG_UA) as r:
        txt = _flat(await r.text())
    res = {}
    for mo in (3, 6, 12):
        m = re.search(rf"\b{mo}\s*months?\b[^\d]{{0,60}}?([\d]+(?:[.,]\d+)?)\s*TON", txt, re.I)
        if m: res[mo] = float(m.group(1).replace(",", "."))
    if len(res) < 3: raise ValueError("قیمت پرمیوم پیدا نشد")
    return res

async def update_fragment_prices():
    """قیمت‌ها رو از فراگمنت می‌گیره، با تون→دلار تبدیل می‌کنه و فقط اگه منطقی بود ذخیره می‌کنه."""
    if P["ton_usdt"] <= 0: return "قیمت تون هنوز نیومده"
    report, errors = [], []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as s:
        try:
            usd = await _frag_stars_ton(s) * P["ton_usdt"]
            if not 0.5 <= usd <= 2.0: raise ValueError(f"قیمت غیرمنطقی برای ۵۰ استارز: {usd:.3f}$")
            await execute("UPDATE settings SET v=? WHERE k='star_gift50_usd'", (f"{usd:.4f}",))
            report.append(f"⭐ گیفت ۵۰تایی: {usd:.3f}$")
        except Exception as e:
            errors.append(f"استارز: {e}")
        try:
            bounds = {3: (5, 30), 6: (8, 45), 12: (15, 80)}
            for mo, ton in (await _frag_premium_ton(s)).items():
                usd = ton * P["ton_usdt"]
                if not bounds[mo][0] <= usd <= bounds[mo][1]: raise ValueError(f"قیمت غیرمنطقی پرمیوم {mo} ماهه: {usd:.2f}$")
            for mo, ton in (await _frag_premium_ton(s)).items():
                await execute("UPDATE settings SET v=? WHERE k=?", (f"{ton * P['ton_usdt']:.3f}", f"premium_usd_{mo}"))
            report.append("💎 پرمیوم بروز شد")
        except Exception as e:
            errors.append(f"پرمیوم: {e}")
    if report: FRAG["ts"] = time.time(); FRAG["alerted"] = False
    if errors and not FRAG["alerted"]:
        FRAG["alerted"] = True
        await to_admins("⚠️ دریافت خودکار قیمت فراگمنت ناموفق بود؛ از آخرین قیمت ذخیره‌شده استفاده میشه.\n" + "\n".join(errors) +
                        "\nدستی: /set star_gift50_usd و /set premium_usd_3 ...")
    FRAG["last"] = "\n".join(report + ["❌ " + e for e in errors])
    return FRAG["last"]

async def fragment_loop():
    while P["ton_usdt"] <= 0: await asyncio.sleep(5)
    while True:
        try: await update_fragment_prices()
        except Exception as e: log.warning("fragment loop: %s", e)
        await asyncio.sleep(600)

async def usdt_toman():
    man = await sf("usdt_toman_manual")
    return man if man > 0 else P["usdt_toman"]

async def price_ready():
    return P["ton_usdt"] > 0 and (await usdt_toman()) > 0 and time.time() - P["ts"] < 900

async def ton_margin(ton):
    if ton > await sf("ton_hi_threshold"): return await sf("ton_fee_pct_hi")
    if ton > await sf("ton_mid_threshold"): return await sf("ton_fee_pct_mid")
    return await sf("ton_fee_pct_lo")

async def ton_quote(ton=None, toman=None):
    """قیمت تون = قیمت بازار (بدون سود). سود شما داخل «کارمزد» میاد: کارمزد = هزینه واقعی انتقال + درصد."""
    base = P["ton_usdt"] * await usdt_toman()
    net = int(round(await sf("ton_net_fee_ton") * base / 100) * 100)   # هزینه واقعی انتقال
    if ton is None:
        t = max(toman - net, 0) / base
        for _ in range(3):
            m = await ton_margin(t)
            t = max(toman - net, 0) / (base * (1 + m / 100))
        ton = math.floor(t * 100) / 100
    m = await ton_margin(ton)
    price = round_to(ton * base)
    fee = int(round((net + ton * base * m / 100) / 100) * 100)
    return {"ton": ton, "rate": int(base), "price": price, "fee": fee, "total": price + fee}

# ── قیمت خرید فراگمنت (خودکار، با fallback دستی) ──
FRAG = {}   # key -> (price_in_ton, ts)
FRAG_ALERT = [0.0]

FRAG_PUB = {}  # قیمت از API عمومی (فقط خواندن)

def frag_get(key):
    for src in (FRAG, FRAG_PUB):
        v = src.get(key)
        if v and time.time() - v[1] < 1800:
            return v[0]
    return None

async def pub_price_loop():
    """قیمت‌های فراگمنت از یه API عمومی (بدون لاگین، فقط GET). موقتی تا وقتی فراگمنت خودت وصل نیست."""
    url = "https://api.fragment-api.space/api/v1/prices"
    while True:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as sess:
                async with sess.get(url) as r:
                    j = await r.json()
            t = time.time()
            FRAG_PUB["stars50"] = (float(j["stars"]["price_per_star_ton"]) * 50, t)
            for mo in (3, 6, 12):
                FRAG_PUB[f"prem{mo}"] = (float(j["premium"][f"{mo}_months"]["base_ton"]), t)
            log.info("public fragment prices: %s", FRAG_PUB)
        except Exception as e:
            log.warning("public price fetch failed: %s", e)
        await asyncio.sleep(300)

async def frag_loop():
    if not (FRAGMENT_HASH and FRAGMENT_COOKIE and FRAGMENT_WALLET):
        log.info("Fragment auto-price disabled (missing env), using manual prices"); return
    from afragment import AsyncFragmentClient, AuthenticationError
    while True:
        jobs = []
        if FRAGMENT_STARS_USER:
            jobs.append(("stars50", lambda c: c.buy_stars(FRAGMENT_STARS_USER.lstrip("@"), 50, FRAGMENT_WALLET)))
        if FRAGMENT_PREMIUM_USER:
            for mo in (3, 6, 12):
                jobs.append((f"prem{mo}", lambda c, mo=mo: c.buy_premium(FRAGMENT_PREMIUM_USER.lstrip("@"), mo, FRAGMENT_WALLET)))
        try:
            async with AsyncFragmentClient(fragment_hash=FRAGMENT_HASH, fragment_cookie=FRAGMENT_COOKIE) as c:
                for key, fn in jobs:
                    try:
                        r = await fn(c)
                        FRAG[key] = (float(r["amount"]), time.time())
                    except AuthenticationError:
                        raise
                    except Exception as e:
                        log.warning("fragment %s failed: %s", key, e)
                    await asyncio.sleep(3)
            log.info("fragment prices: %s", FRAG)
        except AuthenticationError:
            if time.time() - FRAG_ALERT[0] > 3600:
                FRAG_ALERT[0] = time.time()
                await to_admins("⚠️ نشست فراگمنت منقضی شده. FRAGMENT_HASH و FRAGMENT_COOKIE را در .env بروز کنید. "
                                "تا آن موقع قیمت‌های دستی (/prices) استفاده می‌شود.")
        except Exception as e:
            log.warning("fragment loop: %s", e)
        await asyncio.sleep(300)

async def stars_unit_toman():
    ton_price = frag_get("stars50")
    if ton_price:
        return ton_price / 50 * P["ton_usdt"] * await usdt_toman()
    return await sf("star_gift50_usd") / 50 * await usdt_toman()

async def stars_price(n):
    m = 0 if n < await sf("star_free_below") else await sf("star_margin")
    return round_to(n * await stars_unit_toman() * (1 + m / 100))

async def premium_price(months):
    ton_price = frag_get(f"prem{months}")
    cost = ton_price * P["ton_usdt"] * await usdt_toman() if ton_price else await sf(f"premium_usd_{months}") * await usdt_toman()
    return round_to(cost * (1 + await sf("premium_margin") / 100))

# ───────────────────────── notify helpers ─────────────────────────
async def to_admins(text, kb=None, photo=None):
    sent = []
    for a in ADMIN_IDS:
        try:
            if photo:
                msg = await admin_bot.send_photo(a, photo, caption=text, reply_markup=kb)
            else:
                msg = await admin_bot.send_message(a, text, reply_markup=kb)
            sent.append((a, msg.message_id))
        except Exception as e:
            log.warning("admin notify failed %s: %s", a, e)
    return sent

async def to_user(uid, text):
    try:
        await user_bot.send_message(uid, text)
    except Exception as e:
        log.warning("user notify failed %s: %s", uid, e)

async def to_channel(text):
    if not CHANNEL_ID: return
    try:
        await admin_bot.send_message(CHANNEL_ID, text)
    except Exception as e:
        log.warning("channel post failed: %s", e)

def kb_ok_no(prefix, id_):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تایید", callback_data=f"{prefix}:ok:{id_}"),
        InlineKeyboardButton(text="❌ رد", callback_data=f"{prefix}:no:{id_}")]])

# ═════════════════════════ USER BOT ═════════════════════════
ur = Router()

class BanMW(BaseMiddleware):
    async def __call__(self, handler, event, data):
        u = getattr(event, "from_user", None)
        if u:
            r = await fetchone("SELECT banned FROM users WHERE id=?", (u.id,))
            if r and r["banned"]:
                return
        return await handler(event, data)

class BuyTon(StatesGroup): amount = State()
class BuyStars(StatesGroup): target = State(); count = State()
class BuyPrem(StatesGroup): target = State()
class Pay(StatesGroup): receipt = State(); tracking = State()
class Kyc(StatesGroup): card = State(); photo = State()
class Wd(StatesGroup): amount = State(); address = State(); memo = State(); confirm = State()
class Support(StatesGroup): msg = State()

MENU = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
    [KeyboardButton(text="🛒 خرید تون"), KeyboardButton(text="⭐ خرید استارز")],
    [KeyboardButton(text="💎 تلگرام پرمیوم"), KeyboardButton(text="👛 کیف پول")],
    [KeyboardButton(text="📜 تاریخچه"), KeyboardButton(text="💸 برداشت")],
    [KeyboardButton(text="🆘 پشتیبانی")]])

async def get_user(uid):
    return await fetchone("SELECT * FROM users WHERE id=?", (uid,))

async def phone_ok(m: Message):
    u = await get_user(m.from_user.id)
    if u and u["phone"]: return True
    await ask_phone(m); return False

async def ask_phone(m: Message):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True, keyboard=[
        [KeyboardButton(text="📱 اشتراک‌گذاری شماره", request_contact=True)]])
    await m.answer("برای استفاده از ربات، شماره تلفن شما ثبت می‌شود. "
                   "با زدن دکمه‌ی زیر اجازه‌ی ثبت شماره‌تان را می‌دهید.", reply_markup=kb)

@ur.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    await execute("INSERT OR IGNORE INTO users(id,username,created) VALUES(?,?,?)",
                  (m.from_user.id, m.from_user.username, now()))
    u = await get_user(m.from_user.id)
    if not u["phone"]: return await ask_phone(m)
    await m.answer("سلام 👋 به ربات خرید تون، استارز و پرمیوم تلگرام خوش اومدی.\nاز منوی زیر انتخاب کن:", reply_markup=MENU)

@ur.message(F.contact)
async def got_contact(m: Message):
    if m.contact.user_id != m.from_user.id:
        return await m.answer("لطفا فقط شماره‌ی خودتان را با دکمه ارسال کنید.")
    await execute("UPDATE users SET phone=?, username=? WHERE id=?",
                  (m.contact.phone_number, m.from_user.username, m.from_user.id))
    await m.answer("✅ شماره ثبت شد. خوش آمدید!", reply_markup=MENU)

# ── menu (registered first so they work in any state) ──
@ur.message(F.text == "🛒 خرید تون")
async def buy_ton(m: Message, state: FSMContext):
    await state.clear()
    if not await phone_ok(m): return
    if not await price_ready(): return await m.answer("⏳ قیمت‌ها در حال بروزرسانی است، کمی بعد تلاش کنید.")
    q = await ton_quote(ton=1.0)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔢 انتخاب با تعداد تون", callback_data="bton:ton")],
        [InlineKeyboardButton(text="💵 انتخاب با مبلغ (تومان)", callback_data="bton:toman")]])
    await m.answer(f"💎 نرخ لحظه‌ای هر تون: <b>{fm(q['rate'])}</b> تومان\n"
                   f"کارمزد (برای ۱ تون): {fm(q['fee'])} تومان\n\nروش انتخاب را بزنید:", reply_markup=kb)

@ur.message(F.text == "⭐ خرید استارز")
async def buy_stars(m: Message, state: FSMContext):
    await state.clear()
    if not await phone_ok(m): return
    if not await price_ready(): return await m.answer("⏳ قیمت‌ها در حال بروزرسانی است، کمی بعد تلاش کنید.")
    await state.set_state(BuyStars.target)
    await m.answer("آیدی (یوزرنیم) تلگرامی که استارز براش خریده میشه رو بفرست، مثل @username")

@ur.message(F.text == "💎 تلگرام پرمیوم")
async def buy_prem(m: Message, state: FSMContext):
    await state.clear()
    if not await phone_ok(m): return
    if not await price_ready(): return await m.answer("⏳ قیمت‌ها در حال بروزرسانی است، کمی بعد تلاش کنید.")
    rows = []
    for mo in (3, 6, 12):
        rows.append([InlineKeyboardButton(text=f"{mo} ماهه — {fm(await premium_price(mo))} تومان",
                                          callback_data=f"prem:{mo}")])
    await m.answer("پلن پرمیوم را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@ur.message(F.text == "👛 کیف پول")
async def wallet(m: Message, state: FSMContext):
    await state.clear()
    if not await phone_ok(m): return
    u = await get_user(m.from_user.id)
    await m.answer(f"👛 موجودی شما: <b>{u['ton_nano'] / NANO:.4f}</b> TON\n"
                   f"🪪 احراز هویت: {'✅ تایید شده' if u['kyc'] else '❌ انجام نشده'}")

@ur.message(F.text == "📜 تاریخچه")
async def history(m: Message, state: FSMContext):
    await state.clear()
    if not await phone_ok(m): return
    st = {"pending": "⏳ در انتظار", "approved": "✅ تایید", "rejected": "❌ رد", "done": "✅ انجام شد"}
    items = []
    for o in await fetchall("SELECT * FROM orders WHERE user_id=? AND status IN ('pending','approved','rejected') "
                            "ORDER BY id DESC LIMIT 15", (m.from_user.id,)):
        items.append((o["created"], f"🛒 {esc(o['title'])} | {fm(o['toman'])} تومان | {st[o['status']]}"))
    for w in await fetchall("SELECT * FROM withdrawals WHERE user_id=? ORDER BY id DESC LIMIT 15", (m.from_user.id,)):
        items.append((w["created"], f"💸 برداشت {w['nano'] / NANO:.4f} TON | {st[w['status']]}"))
    items.sort(key=lambda x: -x[0])
    if not items: return await m.answer("هنوز تراکنشی ندارید.")
    await m.answer("📜 <b>تاریخچه</b>\n\n" + "\n".join(f"{ts_fmt(t)}\n{txt}\n" for t, txt in items[:15]))

@ur.message(F.text == "🆘 پشتیبانی")
async def support(m: Message, state: FSMContext):
    await state.clear()
    await state.set_state(Support.msg)
    await m.answer("پیام خود را بنویسید تا برای پشتیبانی ارسال شود:")

@ur.message(F.text == "💸 برداشت")
async def wd_start(m: Message, state: FSMContext):
    await state.clear()
    if not await phone_ok(m): return
    u = await get_user(m.from_user.id)
    mn = await sf("wd_min_ton")
    if u["ton_nano"] / NANO < mn:
        return await m.answer(f"موجودی شما کافی نیست. حداقل برداشت {mn} TON است.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💯 برداشت کل موجودی", callback_data="wd:all")]])
    await state.set_state(Wd.amount)
    await m.answer(f"موجودی: <b>{u['ton_nano'] / NANO:.4f}</b> TON\nکارمزد برداشت: {await sf('ton_net_fee_ton')} TON\n"
                   "مقدار برداشت را بنویسید یا دکمه را بزنید:", reply_markup=kb)

# ── support ──
@ur.message(Support.msg, F.text)
async def support_msg(m: Message, state: FSMContext):
    u = await get_user(m.from_user.id)
    sent = await to_admins(f"🆘 پیام پشتیبانی\nاز: {esc(m.from_user.full_name)} (<code>{m.from_user.id}</code>) "
                           f"@{esc(m.from_user.username or '-')}\n📱 {esc(u['phone'] if u else '-')}\n\n{esc(m.text)}\n\n"
                           "↩️ برای پاسخ، روی همین پیام Reply بزنید.")
    for chat, mid in sent:
        await execute("INSERT OR REPLACE INTO support VALUES(?,?,?)", (chat, mid, m.from_user.id))
    await state.clear()
    await m.answer("✅ پیام شما ارسال شد. پاسخ از همین ربات به شما می‌رسد.")

# ── build pending purchase ──
async def build(kind, p):
    if kind == "ton":
        q = await ton_quote(**p["q"])
        if q["ton"] < await sf("ton_min"):
            return None
        text = (f"🧾 <b>پیش‌فاکتور</b>\n\n💎 مقدار: <b>{q['ton']}</b> TON\n"
                f"💱 نرخ هر تون: {fm(q['rate'])} تومان\n"
                f"💰 قیمت تون: {fm(q['price'])} تومان\n🏷 کارمزد: {fm(q['fee'])} تومان\n"
                f"━━━━━━━━\n💳 مبلغ قابل واریز: <b>{fm(q['total'])}</b> تومان")
        return dict(kind="ton", title=f"{q['ton']} TON", details={}, toman=q["total"],
                    nano=int(round(q["ton"] * NANO)), text=text, ts=now())
    if kind == "stars":
        price = await stars_price(p["count"])
        text = (f"🧾 <b>پیش‌فاکتور</b>\n\n⭐ تعداد: <b>{p['count']}</b>\n👤 آیدی: {esc(p['target'])}\n"
                f"━━━━━━━━\n💳 مبلغ قابل واریز: <b>{fm(price)}</b> تومان")
        return dict(kind="stars", title=f"{p['count']} استارز", details={"target": p["target"], "count": p["count"]},
                    toman=price, nano=0, text=text, ts=now())
    if kind == "prem":
        price = await premium_price(p["months"])
        text = (f"🧾 <b>پیش‌فاکتور</b>\n\n💎 پرمیوم {p['months']} ماهه\n👤 آیدی: {esc(p['target'])}\n"
                f"━━━━━━━━\n💳 مبلغ قابل واریز: <b>{fm(price)}</b> تومان")
        return dict(kind="prem", title=f"پرمیوم {p['months']} ماهه",
                    details={"target": p["target"], "months": p["months"]}, toman=price, nano=0, text=text, ts=now())

CONFIRM_KB = InlineKeyboardMarkup(inline_keyboard=[[
    InlineKeyboardButton(text="✅ تایید و پرداخت", callback_data="co:ok"),
    InlineKeyboardButton(text="❌ انصراف", callback_data="co:no")]])

async def show_confirm(msg: Message, state: FSMContext, kind, params):
    pend = await build(kind, params)
    if not pend:
        return await msg.answer(f"حداقل خرید تون {await sf('ton_min')} است.")
    await state.set_state(None)
    await state.update_data(kind=kind, params=params, pending=pend)
    await msg.answer(pend["text"], reply_markup=CONFIRM_KB)

# ── TON ──
@ur.callback_query(F.data.startswith("bton:"))
async def bton_mode(c: CallbackQuery, state: FSMContext):
    mode = c.data.split(":")[1]
    await state.set_state(BuyTon.amount); await state.update_data(mode=mode)
    await c.answer()
    await c.message.answer("چند تون می‌خواهید؟ (عدد بنویسید)" if mode == "ton" else "مبلغ را به تومان بنویسید:")

@ur.message(BuyTon.amount, F.text)
async def bton_amount(m: Message, state: FSMContext):
    v = num(m.text)
    if not v: return await m.answer("یک عدد معتبر وارد کنید.")
    mode = (await state.get_data())["mode"]
    if not await price_ready(): return await m.answer("⏳ قیمت‌ها در حال بروزرسانی است.")
    await show_confirm(m, state, "ton", {"q": {"ton": v} if mode == "ton" else {"toman": int(v)}})

# ── Stars ──
@ur.message(BuyStars.target, F.text)
async def stars_target(m: Message, state: FSMContext):
    t = m.text.strip()
    if not re.fullmatch(r"@?[A-Za-z0-9_]{5,32}", t): return await m.answer("آیدی نامعتبر است. مثل @username بفرستید.")
    await state.update_data(target="@" + t.lstrip("@"))
    await state.set_state(BuyStars.count)
    await m.answer(f"تعداد استارز را بنویسید (حداقل {int(await sf('star_min'))}):")

@ur.message(BuyStars.count, F.text)
async def stars_count(m: Message, state: FSMContext):
    v = num(m.text)
    if not v or v != int(v) or v < await sf("star_min"):
        return await m.answer(f"یک عدد صحیح حداقل {int(await sf('star_min'))} وارد کنید.")
    target = (await state.get_data())["target"]
    await show_confirm(m, state, "stars", {"target": target, "count": int(v)})

# ── Premium ──
@ur.callback_query(F.data.startswith("prem:"))
async def prem_plan(c: CallbackQuery, state: FSMContext):
    await state.set_state(BuyPrem.target); await state.update_data(months=int(c.data.split(":")[1]))
    await c.answer()
    await c.message.answer("آیدی (یوزرنیم) اکانتی که پرمیوم براش فعال میشه رو بفرست، مثل @username")

@ur.message(BuyPrem.target, F.text)
async def prem_target(m: Message, state: FSMContext):
    t = m.text.strip()
    if not re.fullmatch(r"@?[A-Za-z0-9_]{5,32}", t): return await m.answer("آیدی نامعتبر است. مثل @username بفرستید.")
    months = (await state.get_data())["months"]
    await show_confirm(m, state, "prem", {"target": "@" + t.lstrip("@"), "months": months})

# ── confirm ──
@ur.callback_query(F.data == "co:no")
async def co_no(c: CallbackQuery, state: FSMContext):
    await state.clear(); await c.message.edit_reply_markup(reply_markup=None); await c.answer("لغو شد")

@ur.callback_query(F.data == "co:ok")
async def co_ok(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "pending" not in data: return await c.answer("منقضی شد، دوباره شروع کنید.", show_alert=True)
    pend = data["pending"]
    if time.time() - pend["ts"] > 300:
        if not await price_ready(): return await c.answer("قیمت‌ها در حال بروزرسانی است.", show_alert=True)
        pend = await build(data["kind"], data["params"])
        await state.update_data(pending=pend)
        await c.answer()
        return await c.message.answer("⏱ قیمت بروز شد:\n\n" + pend["text"], reply_markup=CONFIRM_KB)
    await c.answer(); await c.message.edit_reply_markup(reply_markup=None)
    await place_order(c.message, state, c.from_user.id, pend)

async def place_order(msg: Message, state: FSMContext, uid, pend):
    u = await get_user(uid)
    if not u["kyc"]:
        used = await fetchone("SELECT COALESCE(SUM(toman),0) s FROM orders WHERE user_id=? AND status IN ('pending','approved')", (uid,))
        limit = int(await sf("kyc_limit"))
        if used["s"] + pend["toman"] > limit:
            await state.set_state(Kyc.card)
            return await msg.answer(f"🪪 برای خرید بیش از {fm(limit)} تومان (مجموع) احراز هویت لازم است.\n"
                                    "ابتدا شماره‌ی ۱۶ رقمی کارت بانکی خود را بفرستید:")
    await execute("UPDATE orders SET status='cancelled' WHERE user_id=? AND status='awaiting'", (uid,))
    ttl = int(await sf("order_ttl_min"))
    oid, _ = await execute("INSERT INTO orders(user_id,kind,title,details,toman,ton_nano,status,created,expires) "
                           "VALUES(?,?,?,?,?,?,'awaiting',?,?)",
                           (uid, pend["kind"], pend["title"], json.dumps(pend["details"]), pend["toman"],
                            pend["nano"], now(), now() + ttl * 60))
    await state.set_state(Pay.receipt); await state.update_data(order_id=oid)
    await msg.answer(f"💳 مبلغ <b>{fm(pend['toman'])}</b> تومان را تا <b>{ttl} دقیقه</b> آینده به کارت زیر واریز کنید:\n\n"
                     f"<code>{CARD_NUMBER}</code>\nبه نام: <b>{esc(CARD_OWNER)}</b>\n\n"
                     "📸 بعد از واریز، <b>عکس فیش</b> را همینجا بفرستید.")

# ── KYC ──
@ur.message(Kyc.card, F.text)
async def kyc_card(m: Message, state: FSMContext):
    c = norm(m.text).replace(" ", "")
    if not re.fullmatch(r"\d{16}", c): return await m.answer("شماره کارت باید ۱۶ رقم باشد.")
    await state.update_data(card=c); await state.set_state(Kyc.photo)
    await m.answer("حالا عکس کارت بانکی (روی کارت) را بفرستید. می‌توانید CVV2 و تاریخ انقضا را بپوشانید.")

@ur.message(Kyc.photo, F.photo)
async def kyc_photo(m: Message, state: FSMContext):
    card = (await state.get_data())["card"]
    kid, _ = await execute("INSERT INTO kyc(user_id,card,status,created) VALUES(?,?, 'pending',?)", (m.from_user.id, card, now()))
    buf = io.BytesIO(); await user_bot.download(m.photo[-1].file_id, destination=buf)
    u = await get_user(m.from_user.id)
    await to_admins(f"🪪 درخواست احراز هویت #{kid}\nکاربر: <code>{m.from_user.id}</code> @{esc(m.from_user.username or '-')}\n"
                    f"📱 {esc(u['phone'])}\n💳 <code>{card}</code>",
                    kb_ok_no("kyc", kid), BufferedInputFile(buf.getvalue(), "card.jpg"))
    await state.clear()
    await m.answer("✅ مدارک ارسال شد. پس از بررسی نتیجه اعلام می‌شود، سپس خرید خود را دوباره انجام دهید.")

# ── receipt ──
@ur.message(Pay.receipt, F.photo)
async def pay_receipt(m: Message, state: FSMContext):
    o = await fetchone("SELECT status FROM orders WHERE id=?", ((await state.get_data())["order_id"],))
    if not o or o["status"] != "awaiting":
        await state.clear(); return await m.answer("این سفارش منقضی یا لغو شده است. دوباره سفارش بدهید.")
    await state.update_data(file_id=m.photo[-1].file_id, uid=m.photo[-1].file_unique_id)
    await state.set_state(Pay.tracking)
    await m.answer("✅ عکس دریافت شد. حالا <b>کد پیگیری</b> تراکنش را بفرستید:")

@ur.message(Pay.receipt)
async def pay_receipt_bad(m: Message):
    await m.answer("لطفا عکس فیش واریزی را بفرستید.")

@ur.message(Pay.tracking, F.text)
async def pay_tracking(m: Message, state: FSMContext):
    data = await state.get_data(); oid = data["order_id"]; code = norm(m.text)
    if not re.fullmatch(r"[A-Za-z0-9\-]{4,40}", code): return await m.answer("کد پیگیری نامعتبر است.")
    o = await fetchone("SELECT * FROM orders WHERE id=?", (oid,))
    if not o or o["status"] != "awaiting":
        await state.clear(); return await m.answer("این سفارش منقضی یا لغو شده است. دوباره سفارش بدهید.")
    dup = await fetchone("SELECT id FROM orders WHERE (tracking=? OR receipt_uid=?) AND status IN ('pending','approved')", (code, data["uid"]))
    if dup: return await m.answer("⚠️ این فیش یا کد پیگیری قبلا ثبت شده است.")
    await execute("UPDATE orders SET status='pending', tracking=?, receipt_uid=? WHERE id=?", (code, data["uid"], oid))
    buf = io.BytesIO(); await user_bot.download(data["file_id"], destination=buf)
    u = await get_user(o["user_id"]); det = json.loads(o["details"])
    extra = f"\n👤 آیدی تحویل: {esc(det['target'])}" if "target" in det else ""
    await to_admins(f"🛒 سفارش #{oid} ({esc(o['kind'])})\n📦 {esc(o['title'])}{extra}\n💰 {fm(o['toman'])} تومان\n"
                    f"🔖 کد پیگیری: <code>{esc(code)}</code>\n👤 کاربر: <code>{o['user_id']}</code> @{esc(u['username'] or '-')}\n"
                    f"📱 {esc(u['phone'])}\n\n⚠️ برای استارز/پرمیوم اول تحویل بدید، بعد تایید بزنید.",
                    kb_ok_no("ord", oid), BufferedInputFile(buf.getvalue(), "receipt.jpg"))
    await state.clear()
    await m.answer("✅ فیش شما ثبت و برای بررسی ارسال شد. نتیجه به شما اطلاع داده می‌شود.", reply_markup=MENU)

# ── withdraw ──
async def wd_set_amount(msg: Message, state: FSMContext, uid, nano):
    u = await get_user(uid); fee = int(await sf("ton_net_fee_ton") * NANO)
    if nano > u["ton_nano"]: return await msg.answer("مقدار بیشتر از موجودی است.")
    if nano / NANO < await sf("wd_min_ton"): return await msg.answer(f"حداقل برداشت {await sf('wd_min_ton')} TON است.")
    if nano <= fee: return await msg.answer("مقدار کمتر از کارمزد است.")
    await state.update_data(nano=nano, fee=fee); await state.set_state(Wd.address)
    await msg.answer("آدرس کیف پول تون (TON) مقصد را بفرستید:")

@ur.callback_query(Wd.amount, F.data == "wd:all")
async def wd_all(c: CallbackQuery, state: FSMContext):
    await c.answer(); u = await get_user(c.from_user.id)
    await wd_set_amount(c.message, state, c.from_user.id, u["ton_nano"])

@ur.message(Wd.amount, F.text)
async def wd_amount(m: Message, state: FSMContext):
    v = num(m.text)
    if not v: return await m.answer("عدد معتبر وارد کنید.")
    await wd_set_amount(m, state, m.from_user.id, int(round(v * NANO)))

@ur.message(Wd.address, F.text)
async def wd_address(m: Message, state: FSMContext):
    a = m.text.strip()
    if not re.fullmatch(r"(?:[EU]Q[A-Za-z0-9_\-]{46}|-?\d+:[0-9a-fA-F]{64})", a):
        return await m.answer("آدرس نامعتبر است. دوباره بفرستید.")
    await state.update_data(address=a); await state.set_state(Wd.memo)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="ممو (Tag) ندارم", callback_data="wd:nomemo")]])
    await m.answer("آیا آدرس مقصد <b>ممو (Memo/Tag)</b> دارد؟ اگر دارد بنویسید، وگرنه دکمه را بزنید:", reply_markup=kb)

async def wd_show(msg: Message, state: FSMContext, memo):
    await state.update_data(memo=memo); await state.set_state(Wd.confirm)
    d = await state.get_data()
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ ثبت درخواست", callback_data="wd:ok"),
        InlineKeyboardButton(text="❌ انصراف", callback_data="wd:cancel")]])
    await msg.answer(f"🧾 <b>درخواست برداشت</b>\n\nمقدار: {d['nano'] / NANO:.4f} TON\nکارمزد: {d['fee'] / NANO:.4f} TON\n"
                     f"دریافتی: <b>{(d['nano'] - d['fee']) / NANO:.4f}</b> TON\nآدرس: <code>{esc(d['address'])}</code>\n"
                     f"ممو: {esc(memo) if memo else 'ندارد'}", reply_markup=kb)

@ur.callback_query(Wd.memo, F.data == "wd:nomemo")
async def wd_nomemo(c: CallbackQuery, state: FSMContext):
    await c.answer(); await wd_show(c.message, state, "")

@ur.message(Wd.memo, F.text)
async def wd_memo(m: Message, state: FSMContext):
    if len(m.text) > 128: return await m.answer("ممو خیلی طولانی است.")
    await wd_show(m, state, m.text.strip())

@ur.callback_query(Wd.confirm, F.data == "wd:cancel")
async def wd_cancel(c: CallbackQuery, state: FSMContext):
    await state.clear(); await c.message.edit_reply_markup(reply_markup=None); await c.answer("لغو شد")

@ur.callback_query(Wd.confirm, F.data == "wd:ok")
async def wd_ok(c: CallbackQuery, state: FSMContext):
    d = await state.get_data(); uid = c.from_user.id
    _, rc = await execute("UPDATE users SET ton_nano=ton_nano-? WHERE id=? AND ton_nano>=?", (d["nano"], uid, d["nano"]))
    if not rc: return await c.answer("موجودی کافی نیست.", show_alert=True)
    wid, _ = await execute("INSERT INTO withdrawals(user_id,nano,fee_nano,address,memo,status,created) VALUES(?,?,?,?,?,'pending',?)",
                           (uid, d["nano"], d["fee"], d["address"], d["memo"], now()))
    u = await get_user(uid)
    await to_admins(f"💸 درخواست برداشت #{wid}\nکاربر: <code>{uid}</code> @{esc(u['username'] or '-')} 📱 {esc(u['phone'])}\n"
                    f"مقدار کل: {d['nano'] / NANO:.4f} TON\nکارمزد: {d['fee'] / NANO:.4f}\n"
                    f"✅ مقدار ارسال: <b>{(d['nano'] - d['fee']) / NANO:.4f}</b> TON\n"
                    f"آدرس: <code>{esc(d['address'])}</code>\nممو: <code>{esc(d['memo']) if d['memo'] else 'ندارد'}</code>",
                    kb_ok_no("wd", wid))
    await state.clear(); await c.message.edit_reply_markup(reply_markup=None); await c.answer()
    await c.message.answer("✅ درخواست برداشت ثبت شد و پس از تایید ارسال می‌شود.", reply_markup=MENU)

# ── background loops ──
async def expiry_loop():
    while True:
        try:
            rows = await fetchall("SELECT id,user_id FROM orders WHERE status='awaiting' AND expires<?", (now(),))
            for r in rows:
                await execute("UPDATE orders SET status='expired' WHERE id=? AND status='awaiting'", (r["id"],))
                await to_user(r["user_id"], f"⌛ سفارش #{r['id']} به دلیل عدم ارسال فیش لغو شد.")
        except Exception as e:
            log.warning("expiry loop: %s", e)
        await asyncio.sleep(60)

async def backup_loop():
    while True:
        try:
            os.makedirs("backups", exist_ok=True)
            dst = f"backups/shop-{datetime.now():%Y%m%d}.db"
            def _b():
                src = sqlite3.connect(DB_PATH); out = sqlite3.connect(dst); src.backup(out); out.close(); src.close()
            await asyncio.to_thread(_b)
            for f in sorted(os.listdir("backups"))[:-14]: os.remove(os.path.join("backups", f))
        except Exception as e:
            log.warning("backup: %s", e)
        await asyncio.sleep(86400)

# ═════════════════════════ ADMIN BOT ═════════════════════════
ar = Router()
ar.message.filter(F.from_user.id.in_(ADMIN_IDS))
ar.callback_query.filter(F.from_user.id.in_(ADMIN_IDS))

async def done(c: CallbackQuery, note):
    await c.message.edit_reply_markup(reply_markup=None)
    await c.message.reply(note); await c.answer()

@ar.callback_query(F.data.startswith("ord:"))
async def adm_order(c: CallbackQuery):
    _, act, oid = c.data.split(":"); oid = int(oid)
    new = "approved" if act == "ok" else "rejected"
    _, rc = await execute("UPDATE orders SET status=? WHERE id=? AND status='pending'", (new, oid))
    if not rc: return await c.answer("قبلا بررسی شده.", show_alert=True)
    o = await fetchone("SELECT * FROM orders WHERE id=?", (oid,))
    if act == "ok":
        if o["kind"] == "ton":
            await execute("UPDATE users SET ton_nano=ton_nano+? WHERE id=?", (o["ton_nano"], o["user_id"]))
            await to_user(o["user_id"], f"✅ سفارش #{oid} تایید شد و {o['ton_nano'] / NANO} TON به کیف پولتان اضافه شد.")
        else:
            await to_user(o["user_id"], f"✅ سفارش #{oid} ({esc(o['title'])}) تایید و تحویل داده شد.")
        await to_channel(f"✅ {esc(o['title'])} به قیمت {fm(o['toman'])} تومان تایید شد")
        await done(c, f"✅ سفارش #{oid} تایید شد.")
    else:
        await to_user(o["user_id"], f"❌ سفارش #{oid} رد شد. در صورت اشتباه با پشتیبانی تماس بگیرید.")
        await done(c, f"❌ سفارش #{oid} رد شد.")

@ar.callback_query(F.data.startswith("wd:"))
async def adm_wd(c: CallbackQuery):
    _, act, wid = c.data.split(":"); wid = int(wid)
    new = "done" if act == "ok" else "rejected"
    _, rc = await execute("UPDATE withdrawals SET status=? WHERE id=? AND status='pending'", (new, wid))
    if not rc: return await c.answer("قبلا بررسی شده.", show_alert=True)
    w = await fetchone("SELECT * FROM withdrawals WHERE id=?", (wid,))
    if act == "ok":
        await to_user(w["user_id"], f"✅ برداشت #{wid} انجام شد ({(w['nano'] - w['fee_nano']) / NANO:.4f} TON).")
        await done(c, f"✅ برداشت #{wid} ثبت شد (مطمئن شو ارسال کردی).")
    else:
        await execute("UPDATE users SET ton_nano=ton_nano+? WHERE id=?", (w["nano"], w["user_id"]))
        await to_user(w["user_id"], f"❌ برداشت #{wid} رد شد و مبلغ به کیف پولتان برگشت.")
        await done(c, f"❌ برداشت #{wid} رد و موجودی برگشت.")

@ar.callback_query(F.data.startswith("kyc:"))
async def adm_kyc(c: CallbackQuery):
    _, act, kid = c.data.split(":"); kid = int(kid)
    new = "approved" if act == "ok" else "rejected"
    _, rc = await execute("UPDATE kyc SET status=? WHERE id=? AND status='pending'", (new, kid))
    if not rc: return await c.answer("قبلا بررسی شده.", show_alert=True)
    k = await fetchone("SELECT * FROM kyc WHERE id=?", (kid,))
    if act == "ok":
        await execute("UPDATE users SET kyc=1 WHERE id=?", (k["user_id"],))
        await to_user(k["user_id"], "✅ احراز هویت شما تایید شد. اکنون می‌توانید خرید کنید.")
    else:
        await to_user(k["user_id"], "❌ احراز هویت شما رد شد. با پشتیبانی تماس بگیرید.")
    await done(c, f"احراز هویت #{kid}: {new}")

@ar.message(F.reply_to_message, F.text)
async def adm_support_reply(m: Message):
    r = await fetchone("SELECT user_id FROM support WHERE chat_id=? AND msg_id=?", (m.chat.id, m.reply_to_message.message_id))
    if not r: return
    await to_user(r["user_id"], f"💬 <b>پشتیبانی:</b>\n{esc(m.text)}")
    await m.reply("✅ ارسال شد.")

@ar.message(CommandStart())
async def adm_start(m: Message):
    await m.answer("پنل ادمین ✅\n/stats\n/fragment (بروزرسانی دستی قیمت فراگمنت)\n/prices\n/set key value\n/setrate تومان_هر_USDT (۰ = خودکار)\n"
                   "/ban id\n/unban id\n/credit id مقدار_تون\n/broadcast متن")

@ar.message(Command("prices"))
async def adm_prices(m: Message):
    rows = [r for r in await fetchall("SELECT k,v FROM settings ORDER BY k") if r["k"] in DEFAULTS]
    ut = await usdt_toman()
    await m.answer(f"TON/USDT: {P['ton_usdt']}\nUSDT/تومان (فعال): {ut:,.0f}\n"
                   f"💎 قیمت بازار هر تون (بدون سود): {P['ton_usdt'] * ut:,.0f} تومان\n"
                   f"آخرین آپدیت: {ts_fmt(P['ts']) if P['ts'] else '-'}\n"
                   f"فراگمنت (TON): " + (", ".join(f"{k}={v[0]}" for k, v in {**FRAG_PUB, **FRAG}.items()) or "دستی/غیرفعال") + "\n\n" + "\n".join(f"{r['k']} = {r['v']}" for r in rows))

@ar.message(Command("fragment"))
async def adm_fragment(m: Message):
    await m.answer("⏳ در حال دریافت از فراگمنت...")
    await m.answer(await update_fragment_prices() or "-")

@ar.message(Command("set"))
async def adm_set(m: Message):
    p = m.text.split()
    if len(p) != 3 or p[1] not in DEFAULTS: return await m.answer("استفاده: /set key value (کلیدها در /prices)")
    await execute("UPDATE settings SET v=? WHERE k=?", (p[2], p[1])); await m.answer("✅ ذخیره شد.")

@ar.message(Command("setrate"))
async def adm_setrate(m: Message):
    p = m.text.split()
    if len(p) != 2: return await m.answer("استفاده: /setrate 95000")
    try: v = float(norm(p[1]))
    except Exception: return await m.answer("عدد معتبر وارد کنید.")
    if v != 0 and not (10000 <= v <= 1000000):
        return await m.answer("عدد غیرمنطقیه. قیمت هر تتر را به تومان بنویسید (مثلا 95000). برای حالت خودکار 0 بزنید.")
    await execute("UPDATE settings SET v=? WHERE k='usdt_toman_manual'", (p[1],)); await m.answer("✅")

@ar.message(Command("ban"))
async def adm_ban(m: Message):
    p = m.text.split()
    await execute("UPDATE users SET banned=1 WHERE id=?", (int(p[1]),)); await m.answer("🚫 بن شد.")

@ar.message(Command("unban"))
async def adm_unban(m: Message):
    p = m.text.split()
    await execute("UPDATE users SET banned=0 WHERE id=?", (int(p[1]),)); await m.answer("✅ آزاد شد.")

@ar.message(Command("credit"))
async def adm_credit(m: Message):
    p = m.text.split()
    await execute("UPDATE users SET ton_nano=ton_nano+? WHERE id=?", (int(float(p[2]) * NANO), int(p[1])))
    await to_user(int(p[1]), f"✅ مبلغ {p[2]} TON به کیف پول شما اضافه شد."); await m.answer("✅")

@ar.message(Command("stats"))
async def adm_stats(m: Message):
    n = (await fetchone("SELECT COUNT(*) c FROM users"))["c"]
    pend = (await fetchone("SELECT COUNT(*) c FROM orders WHERE status='pending'"))["c"]
    tot = (await fetchone("SELECT COALESCE(SUM(toman),0) s FROM orders WHERE status='approved'"))["s"]
    bal = (await fetchone("SELECT COALESCE(SUM(ton_nano),0) s FROM users"))["s"]
    await m.answer(f"👥 کاربران: {n}\n⏳ سفارش در انتظار: {pend}\n💰 مجموع فروش تاییدشده: {fm(tot)} تومان\n"
                   f"👛 مجموع موجودی کاربران: {bal / NANO:.4f} TON")

@ar.message(Command("broadcast"))
async def adm_bc(m: Message):
    text = m.text.partition(" ")[2].strip()
    if not text: return await m.answer("استفاده: /broadcast متن")
    ok = 0
    for r in await fetchall("SELECT id FROM users WHERE banned=0"):
        try: await user_bot.send_message(r["id"], text); ok += 1
        except Exception: pass
        await asyncio.sleep(0.05)
    await m.answer(f"✅ برای {ok} نفر ارسال شد.")

# ═════════════════════════ MAIN ═════════════════════════
async def main():
    await init_db()
    dpu = Dispatcher(storage=MemoryStorage())
    dpu.message.outer_middleware(BanMW()); dpu.callback_query.outer_middleware(BanMW())
    dpu.include_router(ur)
    dpa = Dispatcher(storage=MemoryStorage()); dpa.include_router(ar)
    await asyncio.gather(
        dpu.start_polling(user_bot, handle_signals=False),
        dpa.start_polling(admin_bot, handle_signals=False),
        price_loop(), fragment_loop(), expiry_loop(), backup_loop())

if __name__ == "__main__":
    asyncio.run(main())
