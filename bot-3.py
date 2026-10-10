import asyncio, html, io, json, logging, math, os, re, shutil, sqlite3, time, unicodedata
from datetime import datetime, timedelta, timezone

import aiohttp, aiosqlite
from aiogram import Bot, Dispatcher, F, Router, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import BaseFilter, Command, CommandStart
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
    "kyc_limit": "400000",         # سقف خرید «روزانه» بدون احراز هویت (تومان)؛ ساعت ۱۲ شب ریست میشه
    "ton_source": "channel",       # منبع قیمت دلاری تون: channel (کانال) یا binance
    "ton_toman_manual": "0",       # اگه >0 باشه قیمت بازار هر تون (تومان) دستیه و از دلار حساب نمیشه
    "usdt_toman_manual": "0",      # اگر >0 باشه به جای نوبیتکس استفاده میشه
    "order_ttl_min": "15",
    "disc_pct_ton": "0.5",         # درصد تخفیف (کش‌بک) روی هر خرید تون
    "disc_pct_stars": "0.5",       # استارز (فقط از star_free_below به بالا؛ زیرش سود صفره پس تخفیف هم صفره)
    "disc_pct_prem": "0.5",        # پرمیوم
    "disc_pct_custom": "0.5",      # تخفیف (کش‌بک) محصولات سفارشی
    "sale_pct_ton": "0", "sale_pct_stars": "0", "sale_pct_prem": "0",   # تخفیف ویژه روی قیمت (با /sale)
    "off_ton": "0", "off_stars": "0", "off_prem3": "0", "off_prem6": "0", "off_prem12": "0",   # 1 = ناموجود
    "topup_min": "10000",          # حداقل شارژ ریالی (تومان)
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
CREATE TABLE IF NOT EXISTS discounts(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount INTEGER, reason TEXT, created REAL);
CREATE TABLE IF NOT EXISTS categories(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, active INTEGER DEFAULT 1, created REAL);
CREATE TABLE IF NOT EXISTS products(id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT, title TEXT,
  price_toman INTEGER, needs_target INTEGER DEFAULT 1, active INTEGER DEFAULT 1, sale_pct REAL DEFAULT 0, created REAL);
CREATE UNIQUE INDEX IF NOT EXISTS ux_track ON orders(tracking) WHERE tracking IS NOT NULL AND status IN ('pending','approved');
"""

async def init_db():
    global db
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.executescript(SCHEMA)
    for table, col, ddl in [("users", "toman_balance", "INTEGER DEFAULT 0"),
                            ("users", "discount_balance", "INTEGER DEFAULT 0"),
                            ("orders", "discount", "INTEGER DEFAULT 0"),
                            ("orders", "paid_wallet", "INTEGER DEFAULT 0"),
                            ("orders", "paid_discount", "INTEGER DEFAULT 0"),
                            ("orders", "card_toman", "INTEGER")]:
        async with db.execute(f"PRAGMA table_info({table})") as cur:
            have = {r["name"] for r in await cur.fetchall()}
        if col not in have:
            await db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
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
IR_TZ = timezone(timedelta(hours=3, minutes=30))     # وقت ایران
def day_start():
    n = datetime.now(IR_TZ)
    return n.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()   # ساعت ۱۲ شب
def ts_fmt(t): return datetime.fromtimestamp(t).strftime("%Y/%m/%d %H:%M")

# ───────────────────────── prices ─────────────────────────
P = {"ton_usdt": 0.0, "usdt_toman": 0.0, "ts": 0.0,
     "usdt_toman_chan": 0.0, "ton_toman_chan": 0.0, "ton_usd_chan": 0.0, "chan_ts": 0.0}
TBL = {"stars": {}, "prem": {}, "ts": 0.0, "tried": 0.0}   # جدول قیمت استارز/پرمیوم کانال (تومان)
CHANNEL_URL = os.getenv("PRICE_CHANNEL_URL", "https://t.me/s/TonPriceIran")

def parse_channel(page: str):
    """از پیش‌نمایش عمومی کانال TonPriceIran: تون، تتر، و جدول استارز/پرمیوم (آخرین مورد صفحه = جدیدترین)."""
    t = re.sub(r"<br\s*/?>", "\n", page)
    t = re.sub(r"<[^>]+>", " ", t)
    t = unicodedata.normalize("NFKC", html.unescape(t))      # ارقام بولد ریاضی (𝟭𝟰) -> 14
    t = re.sub("[\u200b-\u200f\u202a-\u202e]", "", t)
    out = {}
    num_ = lambda x: float(x.replace(",", ""))
    m = re.findall(r"گرام\s*\(GRAM\)\s*:\D*?([\d.]+)\s*USD\s*\|\D*?([\d,]+)\s*تومان", t)
    if m: out["ton_usd"], out["ton_toman"] = float(m[-1][0]), num_(m[-1][1])
    m = re.findall(r"تتر\s*\(USDT\)\s*:\D*?([\d.]+)\s*USD\s*\|\D*?([\d,]+)\s*تومان", t)
    if m: out["usdt_toman"] = num_(m[-1][1])
    out["prem"] = {int(a): num_(b) for a, b in re.findall(r"(\d+)\s*ماهه\s*≈\s*([\d,]+)\s*تومان", t)}
    out["stars"] = {int(a): num_(b) for a, b in re.findall(r"(\d+)\s*ستاره\s*≈\s*([\d,]+)\s*تومان", t)}
    ids = [int(x) for x in re.findall(r"TonPriceIran/(\d+)", page)]
    out["min_id"] = min(ids) if ids else 0
    return out

def _apply_tbl(d):
    if d.get("stars") and d.get("prem"):
        TBL["stars"], TBL["prem"], TBL["ts"] = d["stars"], d["prem"], time.time()

async def channel_loop():
    """هر ۵ دقیقه: تون و تتر. جدول استارز/پرمیوم کانال هر ۴ ساعت میاد؛ اگه تو صفحه‌ی آخر نبود چند صفحه عقب‌تر رو می‌گرده."""
    while True:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15),
                                             headers={"User-Agent": "Mozilla/5.0"}) as sess:
                async def get(url):
                    async with sess.get(url) as r: return await r.text()
                d = parse_channel(await get(CHANNEL_URL))
                if 10000 <= d.get("usdt_toman", 0) <= 1000000 and d.get("ton_toman", 0) > 0:
                    P.update(usdt_toman_chan=d["usdt_toman"], ton_toman_chan=d["ton_toman"],
                             ton_usd_chan=d.get("ton_usd", 0.0), chan_ts=time.time())
                    log.info("channel: usdt=%s ton=%s", d["usdt_toman"], d["ton_toman"])
                else:
                    log.warning("channel: ton/usdt not found")
                _apply_tbl(d)
                if not TBL["prem"] and time.time() - TBL["tried"] > 1800 or \
                        (time.time() - TBL["ts"] > 5 * 3600 and time.time() - TBL["tried"] > 1800):
                    TBL["tried"] = time.time()
                    for _ in range(6):
                        if not d.get("min_id"): break
                        d = parse_channel(await get(f"{CHANNEL_URL}?before={d['min_id']}"))
                        _apply_tbl(d)
                        if time.time() - TBL["ts"] < 60: break
                    log.info("channel table: stars=%s prem=%s", TBL["stars"], TBL["prem"])
        except Exception as e:
            log.warning("channel fetch failed: %s", e)
        await asyncio.sleep(300)

async def price_loop():
    while True:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
                async with s.get("https://api.binance.com/api/v3/ticker/price?symbol=TONUSDT") as r:
                    P["ton_usdt"] = float((await r.json())["price"])
                P["ts"] = time.time()
                try:
                    async with s.get("https://api.nobitex.ir/v3/orderbook/USDTIRT") as r:
                        P["usdt_toman"] = float((await r.json())["lastTradePrice"]) / 10
                except Exception as e:
                    log.warning("nobitex failed: %s", e)
        except Exception as e:
            log.warning("price update failed: %s", e)
        await asyncio.sleep(180)

async def usdt_toman():
    """اولویت: نرخ دستی (/setrate) > کانال TonPriceIran > نوبیتکس"""
    man = await sf("usdt_toman_manual")
    if man > 0: return man
    if P["usdt_toman_chan"] > 0 and time.time() - P["chan_ts"] < 7200: return P["usdt_toman_chan"]
    return P["usdt_toman"]

async def ton_base():
    """قیمت بازار هر تون به تومان (بدون سود): دستی > کانال (مستقیم به تومان) > بایننس × تتر"""
    man = await sf("ton_toman_manual")
    if man > 0: return man
    chan = P["ton_toman_chan"] if P["ton_toman_chan"] > 0 and time.time() - P["chan_ts"] < 7200 else 0.0
    ut = await usdt_toman()
    binance = P["ton_usdt"] * ut if P["ton_usdt"] > 0 and ut > 0 and time.time() - P["ts"] < 900 else 0.0
    first, second = (chan, binance) if await sget("ton_source") == "channel" else (binance, chan)
    return first or second

async def price_ready():
    return await ton_base() > 0 and await usdt_toman() > 0

async def ton_margin(ton):
    if ton > await sf("ton_hi_threshold"): return await sf("ton_fee_pct_hi")
    if ton > await sf("ton_mid_threshold"): return await sf("ton_fee_pct_mid")
    return await sf("ton_fee_pct_lo")

async def ton_quote(ton=None, toman=None):
    """قیمت تون = قیمت بازار (بدون سود). سود شما داخل «کارمزد» میاد: کارمزد = هزینه واقعی انتقال + درصد."""
    base = await ton_base()
    net = int(round(await sf("ton_net_fee_ton") * base / 100) * 100)   # هزینه واقعی انتقال
    if ton is None:
        t = max(toman - net, 0) / base
        for _ in range(3):
            m = await ton_margin(t)
            t = max(toman - net, 0) / (base * (1 + m / 100))
        ton = math.floor(t * 1000) / 1000
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

def tbl_fresh():
    return TBL["ts"] > 0 and time.time() - TBL["ts"] < 86400

async def stars_unit_toman():
    if tbl_fresh() and TBL["stars"]:                    # قیمت کانال (دلار فراگمنت × تتر)
        k = max(TBL["stars"]); return TBL["stars"][k] / k
    ton_price = frag_get("stars50")
    if ton_price and await ton_base() > 0:
        return ton_price / 50 * await ton_base()
    return await sf("star_gift50_usd") / 50 * await usdt_toman()

async def stars_price(n):
    m = 0 if n < await sf("star_free_below") else await sf("star_margin")
    return round_to(n * await stars_unit_toman() * (1 + m / 100))

async def premium_price(months):
    if tbl_fresh() and TBL["prem"].get(months):
        cost = TBL["prem"][months]
    else:
        ton_price = frag_get(f"prem{months}")
        cost = ton_price * await ton_base() if ton_price and await ton_base() > 0 else await sf(f"premium_usd_{months}") * await usdt_toman()
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
class Topup(StatesGroup): amount = State()
class CustomBuy(StatesGroup): target = State()

BASE_ROWS = [["🛒 خرید تون", "⭐ خرید استارز"], ["💎 تلگرام پرمیوم", "👤 حساب من"], ["📜 تاریخچه", "💸 برداشت"]]
RESERVED_TITLES = {t for r in BASE_ROWS for t in r} | {"🛍 سایر محصولات", "🆘 پشتیبانی", "🎛 پنل مدیریت"}

async def menu_categories():
    """دکمه‌های اضافه‌ی منو: «سایر محصولات» + دسته‌هایی که ادمین ساخته (فقط اگه حداقل یک محصول دارن)"""
    out = []
    if (await fetchone("SELECT COUNT(*) c FROM products WHERE category='other'"))["c"]:
        out.append("🛍 سایر محصولات")
    for c in await fetchall("SELECT * FROM categories WHERE active=1 ORDER BY id"):
        if (await fetchone("SELECT COUNT(*) c FROM products WHERE category=?", (f"c{c['id']}",)))["c"]:
            out.append(c["title"])
    return out

async def main_menu():
    rows = [[KeyboardButton(text=t) for t in r] for r in BASE_ROWS]
    cats = await menu_categories()
    for i in range(0, len(cats), 2):
        rows.append([KeyboardButton(text=t) for t in cats[i:i + 2]])
    rows.append([KeyboardButton(text="🆘 پشتیبانی")])
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=rows)

class CatFilter(BaseFilter):
    """پیام = اسم یکی از دکمه‌های دسته‌ای که ادمین ساخته"""
    async def __call__(self, message: Message):
        if not getattr(message, "text", None): return False
        r = await fetchone("SELECT * FROM categories WHERE active=1 AND title=?", (message.text.strip(),))
        return {"cat": r} if r else False

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
    await m.answer("سلام 👋 به ربات خرید تون، استارز و پرمیوم تلگرام خوش اومدی.\nاز منوی زیر انتخاب کن:", reply_markup=await main_menu())

@ur.message(F.contact)
async def got_contact(m: Message):
    if m.contact.user_id != m.from_user.id:
        return await m.answer("لطفا فقط شماره‌ی خودتان را با دکمه ارسال کنید.")
    await execute("UPDATE users SET phone=?, username=? WHERE id=?",
                  (m.contact.phone_number, m.from_user.username, m.from_user.id))
    await m.answer("✅ شماره ثبت شد. خوش آمدید!", reply_markup=await main_menu())

async def available(key):
    return (await sget(f"off_{key}")) != "1"

def prod_eff_price(pr):
    sale = round((pr["price_toman"] * (pr["sale_pct"] or 0) / 100) / 100) * 100
    return int(pr["price_toman"] - sale)

async def products_kb(category, extra_rows=None):
    rows = list(extra_rows or [])
    for pr in await fetchall("SELECT * FROM products WHERE category=? ORDER BY id", (category,)):
        if pr["active"]:
            rows.append([InlineKeyboardButton(text=f"{pr['title']} — {fm(prod_eff_price(pr))} تومان", callback_data=f"cp:{pr['id']}")])
        else:
            rows.append([InlineKeyboardButton(text=f"⛔ {pr['title']} — ناموجود", callback_data="na")])
    return rows

@ur.callback_query(F.data == "na")
async def not_available(c: CallbackQuery):
    await c.answer("⛔ این محصول فعلا موجود نیست.", show_alert=True)

# ── menu (registered first so they work in any state) ──
@ur.message(CatFilter())
async def open_cat(m: Message, state: FSMContext, cat):
    await state.clear()
    if not await phone_ok(m): return
    rows = await products_kb(f"c{cat['id']}")
    if not rows: return await m.answer("فعلا محصولی ثبت نشده است.")
    await m.answer(esc(cat["title"]) + ":", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@ur.message(F.text == "🛒 خرید تون")
async def buy_ton(m: Message, state: FSMContext):
    await state.clear()
    if not await phone_ok(m): return
    if not await available("ton"): return await m.answer("⛔ فعلا خرید تون موجود نیست.")
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
    if not await available("stars"): return await m.answer("⛔ فعلا خرید استارز موجود نیست.")
    if not await price_ready(): return await m.answer("⏳ قیمت‌ها در حال بروزرسانی است، کمی بعد تلاش کنید.")
    await state.set_state(BuyStars.target)
    await m.answer("آیدی (یوزرنیم) تلگرامی که استارز براش خریده میشه رو بفرست، مثل @username")

@ur.message(F.text == "💎 تلگرام پرمیوم")
async def buy_prem(m: Message, state: FSMContext):
    await state.clear()
    if not await phone_ok(m): return
    rows = []
    ready = await price_ready()
    for mo in (3, 6, 12):
        if not await available(f"prem{mo}"):
            rows.append([InlineKeyboardButton(text=f"⛔ {mo} ماهه — ناموجود", callback_data="na")])
        elif ready:
            rows.append([InlineKeyboardButton(text=f"{mo} ماهه — {fm(await premium_price_final(mo))} تومان",
                                              callback_data=f"prem:{mo}")])
    rows = await products_kb("prem", rows)
    if not rows: return await m.answer("⏳ قیمت‌ها در حال بروزرسانی است، کمی بعد تلاش کنید.")
    await m.answer("پلن پرمیوم را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@ur.message(F.text == "🛍 سایر محصولات")
async def others(m: Message, state: FSMContext):
    await state.clear()
    if not await phone_ok(m): return
    rows = await products_kb("other")
    if not rows: return await m.answer("فعلا محصولی ثبت نشده است.")
    await m.answer("🛍 محصولات:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@ur.callback_query(F.data.startswith("cp:"))
async def cp_pick(c: CallbackQuery, state: FSMContext):
    pr = await fetchone("SELECT * FROM products WHERE id=?", (int(c.data.split(":")[1]),))
    if not pr or not pr["active"]: return await c.answer("⛔ این محصول فعلا موجود نیست.", show_alert=True)
    await c.answer()
    if pr["needs_target"]:
        await state.set_state(CustomBuy.target); await state.update_data(pid=pr["id"])
        return await c.message.answer("آیدی (یوزرنیم) تلگرام گیرنده را بفرستید، مثل @username")
    await show_confirm(c.message, state, "custom", {"pid": pr["id"]})

@ur.message(CustomBuy.target, F.text)
async def cp_target(m: Message, state: FSMContext):
    t = m.text.strip()
    if not re.fullmatch(r"@?[A-Za-z0-9_]{5,32}", t): return await m.answer("آیدی نامعتبر است. مثل @username بفرستید.")
    pid = (await state.get_data())["pid"]
    await show_confirm(m, state, "custom", {"pid": pid, "target": "@" + t.lstrip("@")})

async def card_used(uid):
    """مجموع مبلغ کارتی امروز (از ساعت ۱۲ شب به وقت ایران)"""
    r = await fetchone("SELECT COALESCE(SUM(COALESCE(card_toman,toman)),0) s FROM orders "
                       "WHERE user_id=? AND status IN ('pending','approved') AND created>=?", (uid, day_start()))
    return r["s"]

@ur.message(F.text == "👤 حساب من")
async def account(m: Message, state: FSMContext):
    await state.clear()
    if not await phone_ok(m): return
    u = await get_user(m.from_user.id)
    limit = int(await sf("kyc_limit")); used = await card_used(m.from_user.id)
    if u["kyc"]:
        kyc = "✅ تایید شده (بدون سقف)"
    else:
        kyc = (f"❌ انجام نشده\n   سقف خرید روزانه: امروز {fm(max(limit - used, 0))} از {fm(limit)} تومان باقی مانده "
               "(هر شب ساعت ۱۲ ریست می‌شود)")
    buys = await fetchone("SELECT COALESCE(SUM(toman),0) s, COUNT(*) c FROM orders WHERE user_id=? AND status='approved' AND kind!='topup'", (m.from_user.id,))
    rows = [[InlineKeyboardButton(text="➕ شارژ کیف پول ریالی", callback_data="acc:topup")],
            [InlineKeyboardButton(text="🎁 تخفیف‌های من", callback_data="acc:disc")]]
    if not u["kyc"]:
        rows.append([InlineKeyboardButton(text="🪪 احراز هویت", callback_data="acc:kyc")])
    await m.answer(f"👤 <b>حساب من</b>\n\n📱 {esc(u['phone'])}\n🪪 احراز هویت: {kyc}\n\n"
                   f"💎 موجودی تون: <b>{u['ton_nano'] / NANO:.4f}</b> TON\n"
                   f"💵 موجودی ریالی: <b>{fm(u['toman_balance'] or 0)}</b> تومان\n"
                   f"🎁 تخفیف‌ها: <b>{fm(u['discount_balance'] or 0)}</b> تومان\n\n"
                   f"🛍 خریدهای تاییدشده: {buys['c']} عدد، مجموع {fm(buys['s'])} تومان\n\n"
                   "💡 موجودی ریالی و تخفیف‌ها در خرید بعدی خودکار از مبلغ کسر می‌شوند.",
                   reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@ur.callback_query(F.data == "acc:topup")
async def acc_topup(c: CallbackQuery, state: FSMContext):
    await c.answer(); await state.set_state(Topup.amount)
    await c.message.answer(f"مبلغ شارژ را به تومان بنویسید (حداقل {fm(await sf('topup_min'))}):")

@ur.message(Topup.amount, F.text)
async def topup_amount(m: Message, state: FSMContext):
    v = num(m.text)
    if not v or v != int(v): return await m.answer("یک عدد معتبر (تومان) وارد کنید.")
    if v < await sf("topup_min"): return await m.answer(f"حداقل شارژ {fm(await sf('topup_min'))} تومان است.")
    await show_confirm(m, state, "topup", {"amount": int(v)})

@ur.callback_query(F.data == "acc:kyc")
async def acc_kyc(c: CallbackQuery, state: FSMContext):
    await c.answer(); await state.set_state(Kyc.card)
    await c.message.answer("🪪 شماره‌ی ۱۶ رقمی کارت بانکی خود را بفرستید:")

@ur.callback_query(F.data == "acc:disc")
async def acc_disc(c: CallbackQuery):
    await c.answer()
    u = await get_user(c.from_user.id)
    rows = await fetchall("SELECT * FROM discounts WHERE user_id=? ORDER BY id DESC LIMIT 10", (c.from_user.id,))
    lines = [f"{'➕' if r['amount'] > 0 else '➖'} {fm(abs(r['amount']))} تومان — {esc(r['reason'])}\n   {ts_fmt(r['created'])}" for r in rows]
    await c.message.answer(f"🎁 <b>تخفیف‌های من</b>\n\nموجودی: <b>{fm(u['discount_balance'] or 0)}</b> تومان\n"
                           "از هر خرید بخشی به‌صورت تخفیف به اینجا اضافه می‌شود و در خرید بعدی خودکار کسر می‌شود.\n\n"
                           + ("\n".join(lines) if lines else "هنوز تخفیفی ندارید."))

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
    fee = await sf("ton_net_fee_ton")
    if u["ton_nano"] / NANO <= fee:
        return await m.answer(f"موجودی شما کافی نیست. کارمزد انتقال {fee} TON است.")
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
PAY_RE = re.compile(r"💳 مبلغ قابل واریز: <b>[\d,]+</b> تومان")

async def kind_available(kind, p):
    if kind in ("ton", "stars"): return await available(kind)
    if kind == "prem": return await available(f"prem{p['months']}")
    if kind == "custom":
        pr = await fetchone("SELECT active FROM products WHERE id=?", (p["pid"],))
        return bool(pr and pr["active"])
    return True

async def sale_pct(kind, p):
    if kind == "custom":
        pr = await fetchone("SELECT sale_pct FROM products WHERE id=?", (p["pid"],))
        return float(pr["sale_pct"] or 0) if pr else 0.0
    key = {"ton": "sale_pct_ton", "stars": "sale_pct_stars", "prem": "sale_pct_prem"}.get(kind)
    return await sf(key) if key else 0.0

async def premium_price_final(months):
    price = await premium_price(months); pct = await sf("sale_pct_prem")
    return price - int(round(price * pct / 100 / 100) * 100) if pct > 0 else price

async def calc_discount(kind, toman, p):
    key = {"ton": "disc_pct_ton", "stars": "disc_pct_stars", "prem": "disc_pct_prem", "custom": "disc_pct_custom"}.get(kind)
    if not key: return 0
    if kind == "stars" and p["count"] < await sf("star_free_below"): return 0   # زیر آستانه سود صفره
    return int(round(toman * await sf(key) / 100 / 100) * 100)

async def build(kind, p):
    pend = await _build(kind, p)
    if not pend: return None
    pct = await sale_pct(kind, p)
    if pct > 0 and kind != "topup":                        # تخفیف ویژه ادمین روی قیمت
        sale = int(round(pend["toman"] * pct / 100 / 100) * 100)
        if sale > 0:
            new = pend["toman"] - sale
            pend["text"] = PAY_RE.sub(lambda m_: f"🔥 تخفیف ویژه ({pct:g}٪): <b>-{fm(sale)}</b> تومان\n"
                                                 f"💳 مبلغ قابل واریز: <b>{fm(new)}</b> تومان", pend["text"])
            pend["toman"] = new
    pend["discount"] = await calc_discount(kind, pend["toman"], p)
    if pend["discount"] > 0:
        pend["text"] += (f"\n\n🎁 با این خرید <b>{fm(pend['discount'])}</b> تومان تخفیف می‌گیرید "
                         "(بعد از تایید به کیف پول شما، بخش تخفیف‌ها، اضافه می‌شود)")
    return pend

async def _build(kind, p):
    if kind == "custom":
        pr = await fetchone("SELECT * FROM products WHERE id=?", (p["pid"],))
        if not pr or not pr["active"]: return None
        tgt = f"\n👤 آیدی: {esc(p['target'])}" if p.get("target") else ""
        text = (f"🧾 <b>پیش‌فاکتور</b>\n\n📦 {esc(pr['title'])}{tgt}\n"
                f"━━━━━━━━\n💳 مبلغ قابل واریز: <b>{fm(pr['price_toman'])}</b> تومان")
        det = {"pid": pr["id"]}
        if p.get("target"): det["target"] = p["target"]
        return dict(kind="custom", title=pr["title"], details=det, toman=int(pr["price_toman"]), nano=0, text=text, ts=now())
    if kind == "topup":
        a = int(p["amount"])
        text = (f"🧾 <b>شارژ کیف پول ریالی</b>\n\n💵 مبلغ: <b>{fm(a)}</b> تومان\n"
                f"━━━━━━━━\n💳 مبلغ قابل واریز: <b>{fm(a)}</b> تومان")
        return dict(kind="topup", title="شارژ کیف پول", details={}, toman=a, nano=0, text=text, ts=now())
    if kind == "ton":
        q = await ton_quote(**p["q"])
        if q["ton"] <= 0:
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
        return await msg.answer("مقدار یا مبلغ خیلی کم است، عدد بزرگ‌تری وارد کنید.")
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
    if not await available(f"prem{c.data.split(':')[1]}"):
        return await c.answer("⛔ این محصول فعلا موجود نیست.", show_alert=True)
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
    if not await kind_available(data["kind"], data["params"]):
        return await c.answer("⛔ این محصول فعلا موجود نیست.", show_alert=True)
    if time.time() - pend["ts"] > 300:
        if data["kind"] in ("ton", "stars", "prem") and not await price_ready():
            return await c.answer("قیمت‌ها در حال بروزرسانی است.", show_alert=True)
        pend = await build(data["kind"], data["params"])
        if not pend: return await c.answer("این محصول دیگر در دسترس نیست.", show_alert=True)
        await state.update_data(pending=pend)
        await c.answer()
        return await c.message.answer("⏱ قیمت بروز شد:\n\n" + pend["text"], reply_markup=CONFIRM_KB)
    await c.answer(); await c.message.edit_reply_markup(reply_markup=None)
    await place_order(c.message, state, c.from_user.id, pend)

async def place_order(msg: Message, state: FSMContext, uid, pend):
    u = await get_user(uid)
    total = pend["toman"]
    use_d = use_w = 0
    if pend["kind"] != "topup":                       # تخفیف‌ها و موجودی ریالی خودکار کسر میشن
        use_d = min(u["discount_balance"] or 0, total)
        use_w = min(u["toman_balance"] or 0, total - use_d)
    card = total - use_d - use_w
    if not u["kyc"] and card > 0:                     # فقط مبلغ کارتی در سقف احراز هویت حساب میشه
        limit = int(await sf("kyc_limit"))
        if await card_used(uid) + card > limit:
            await state.set_state(Kyc.card)
            left = max(limit - await card_used(uid), 0)
            return await msg.answer(f"🪪 سقف خرید روزانه بدون احراز هویت {fm(limit)} تومان است (امروز {fm(left)} تومان باقی مانده؛ "
                                    "هر شب ساعت ۱۲ ریست می‌شود). برای خرید بیشتر احراز هویت لازم است.\n"
                                    "ابتدا شماره‌ی ۱۶ رقمی کارت بانکی خود را بفرستید:")
    await execute("UPDATE orders SET status='cancelled' WHERE user_id=? AND status='awaiting'", (uid,))
    ttl = int(await sf("order_ttl_min"))
    oid, _ = await execute("INSERT INTO orders(user_id,kind,title,details,toman,ton_nano,status,created,expires,"
                           "discount,paid_wallet,paid_discount,card_toman) VALUES(?,?,?,?,?,?,'awaiting',?,?,?,?,?,?)",
                           (uid, pend["kind"], pend["title"], json.dumps(pend["details"]), total, pend["nano"],
                            now(), now() + ttl * 60, pend.get("discount", 0), use_w, use_d, card))
    if card == 0:                                     # کاملا از موجودی پرداخت میشه
        r = await submit_order(oid)
        await state.clear()
        if r == "ok":
            return await msg.answer("✅ سفارش شما از موجودی کیف پول ثبت و برای بررسی ارسال شد.", reply_markup=await main_menu())
        return await msg.answer("موجودی شما تغییر کرده، لطفا دوباره سفارش بدهید.")
    await state.set_state(Pay.receipt); await state.update_data(order_id=oid)
    lines = [f"💰 مبلغ سفارش: {fm(total)} تومان"]
    if use_d: lines.append(f"🎁 کسر از تخفیف‌ها: -{fm(use_d)}")
    if use_w: lines.append(f"💵 کسر از موجودی ریالی: -{fm(use_w)}")
    await msg.answer("\n".join(lines) + f"\n\n💳 مبلغ <b>{fm(card)}</b> تومان را تا <b>{ttl} دقیقه</b> آینده به کارت زیر واریز کنید:\n\n"
                     f"<code>{CARD_NUMBER}</code>\nبه نام: <b>{esc(CARD_OWNER)}</b>\n\n"
                     "📸 بعد از واریز، <b>عکس فیش</b> را همینجا بفرستید.")

async def submit_order(oid, photo=None, code=None, ruid=None):
    """کسر موجودی‌ها (اتمیک) + ثبت سفارش و ارسال برای ادمین. خروجی: ok / expired / balance"""
    o = await fetchone("SELECT * FROM orders WHERE id=?", (oid,))
    if not o or o["status"] != "awaiting": return "expired"
    uid = o["user_id"]
    if o["paid_discount"]:
        _, rc = await execute("UPDATE users SET discount_balance=discount_balance-? WHERE id=? AND discount_balance>=?",
                              (o["paid_discount"], uid, o["paid_discount"]))
        if not rc: return "balance"
    if o["paid_wallet"]:
        _, rc = await execute("UPDATE users SET toman_balance=toman_balance-? WHERE id=? AND toman_balance>=?",
                              (o["paid_wallet"], uid, o["paid_wallet"]))
        if not rc:
            if o["paid_discount"]:
                await execute("UPDATE users SET discount_balance=discount_balance+? WHERE id=?", (o["paid_discount"], uid))
            return "balance"
    if o["paid_discount"]:
        await execute("INSERT INTO discounts(user_id,amount,reason,created) VALUES(?,?,?,?)",
                      (uid, -o["paid_discount"], f"استفاده در سفارش #{oid}", now()))
    await execute("UPDATE orders SET status='pending', tracking=?, receipt_uid=? WHERE id=?", (code, ruid, oid))
    u = await get_user(uid); det = json.loads(o["details"])
    extra = f"\n👤 آیدی تحویل: {esc(det['target'])}" if "target" in det else ""
    pay = (f"💳 کارت: {fm(o['card_toman'])} | 💵 ریالی: {fm(o['paid_wallet'])} | 🎁 تخفیف: {fm(o['paid_discount'])}")
    tail = f"🔖 کد پیگیری: <code>{esc(code)}</code>\n" if code else "✅ کامل از موجودی کیف پول پرداخت شده (بدون فیش)\n"
    note = ("💵 بعد از تایید به موجودی ریالی کاربر اضافه میشه." if o["kind"] == "topup"
            else "⚠️ برای استارز/پرمیوم اول تحویل بدید، بعد تایید بزنید.")
    await to_admins(f"🛒 سفارش #{oid} ({esc(o['kind'])})\n📦 {esc(o['title'])}{extra}\n💰 {fm(o['toman'])} تومان\n{pay}\n"
                    f"{tail}👤 کاربر: <code>{uid}</code> @{esc(u['username'] or '-')}\n📱 {esc(u['phone'])}\n\n{note}",
                    kb_ok_no("ord", oid), photo)
    return "ok"

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
    buf = io.BytesIO(); await user_bot.download(data["file_id"], destination=buf)
    r = await submit_order(oid, BufferedInputFile(buf.getvalue(), "receipt.jpg"), code, data["uid"])
    await state.clear()
    if r == "ok":
        return await m.answer("✅ فیش شما ثبت و برای بررسی ارسال شد. نتیجه به شما اطلاع داده می‌شود.", reply_markup=await main_menu())
    if r == "balance":
        return await m.answer("موجودی کیف پول/تخفیف شما در این فاصله تغییر کرده. لطفا سفارش را دوباره ثبت کنید.", reply_markup=await main_menu())
    await m.answer("این سفارش منقضی یا لغو شده است. دوباره سفارش بدهید.", reply_markup=await main_menu())

# ── withdraw ──
async def wd_set_amount(msg: Message, state: FSMContext, uid, nano):
    u = await get_user(uid); fee = int(await sf("ton_net_fee_ton") * NANO)
    if nano > u["ton_nano"]: return await msg.answer("مقدار بیشتر از موجودی است.")
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
    await c.message.answer("✅ درخواست برداشت ثبت شد و پس از تایید ارسال می‌شود.", reply_markup=await main_menu())

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
    uid = o["user_id"]
    if act == "ok":
        if o["kind"] == "topup":
            await execute("UPDATE users SET toman_balance=toman_balance+? WHERE id=?", (o["toman"], uid))
            await to_user(uid, f"✅ شارژ {fm(o['toman'])} تومان به کیف پول ریالی شما اضافه شد.")
        else:
            if o["kind"] == "ton":
                await execute("UPDATE users SET ton_nano=ton_nano+? WHERE id=?", (o["ton_nano"], uid))
                msg = f"✅ سفارش #{oid} تایید شد و {o['ton_nano'] / NANO} TON به کیف پولتان اضافه شد."
            else:
                msg = f"✅ سفارش #{oid} ({esc(o['title'])}) تایید و تحویل داده شد."
            d = o["discount"] or 0
            if d > 0:
                await execute("UPDATE users SET discount_balance=discount_balance+? WHERE id=?", (d, uid))
                await execute("INSERT INTO discounts(user_id,amount,reason,created) VALUES(?,?,?,?)",
                              (uid, d, f"تخفیف خرید #{oid}", now()))
                msg += f"\n🎁 {fm(d)} تومان تخفیف به کیف پول شما (بخش تخفیف‌ها) اضافه شد."
            await to_user(uid, msg)
            await to_channel(f"✅ {esc(o['title'])} به قیمت {fm(o['toman'])} تومان تایید شد")
        await done(c, f"✅ سفارش #{oid} تایید شد.")
    else:
        if o["paid_wallet"]:
            await execute("UPDATE users SET toman_balance=toman_balance+? WHERE id=?", (o["paid_wallet"], uid))
        if o["paid_discount"]:
            await execute("UPDATE users SET discount_balance=discount_balance+? WHERE id=?", (o["paid_discount"], uid))
            await execute("INSERT INTO discounts(user_id,amount,reason,created) VALUES(?,?,?,?)",
                          (uid, o["paid_discount"], f"بازگشت از سفارش رد شده #{oid}", now()))
        back = " موجودی و تخفیف استفاده‌شده به حساب شما برگشت." if (o["paid_wallet"] or o["paid_discount"]) else ""
        await to_user(uid, f"❌ سفارش #{oid} رد شد.{back} در صورت اشتباه با پشتیبانی تماس بگیرید.")
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
async def adm_start(m: Message, state: FSMContext):
    await state.clear()
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text="🎛 پنل مدیریت")]])
    await m.answer("سلام ادمین ✅\n\n🎛 برای مدیریت دکمه‌ها، محصولات، تخفیف و موجودی، «پنل مدیریت» را بزنید.\n\n"
                   "دستورها:\n/stats\n/prices\n/set key value\n/setrate تومان_هر_USDT (۰ = خودکار)\n/setton قیمت_تون (۰ = خودکار)\n"
                   "/ban id\n/unban id\n/credit id مقدار_تون\n/creditrial id تومان (موجودی ریالی)\n/creditdisc id تومان (تخفیف)\n"
                   "/broadcast متن\n/cancel (انصراف از مرحله‌ی جاری)", reply_markup=kb)

@ar.message(Command("cancel"))
async def adm_cancel(m: Message, state: FSMContext):
    await state.clear(); await m.answer("انصراف داده شد.")

@ar.message(Command("prices"))
async def adm_prices(m: Message):
    rows = [r for r in await fetchall("SELECT k,v FROM settings ORDER BY k") if r["k"] in DEFAULTS and not r["k"].startswith("off_")]
    ut = await usdt_toman(); tb = await ton_base()
    ch = ts_fmt(P["chan_ts"]) if P["chan_ts"] else "هنوز نخونده"
    st = ", ".join(f"{k}={v:,.0f}" for k, v in sorted(TBL["stars"].items())) or "-"
    pr = ", ".join(f"{k}m={v:,.0f}" for k, v in sorted(TBL["prem"].items())) or "-"
    await m.answer(f"💲 نرخ تتر فعال: {ut:,.0f} تومان (کانال: {P['usdt_toman_chan']:,.0f})\n"
                   f"💎 قیمت بازار هر تون (بدون سود): {tb:,.0f} تومان\n"
                   f"   کانال: {P['ton_toman_chan']:,.0f} ({P['ton_usd_chan']}$) | بایننس: {P['ton_usdt']}$\n"
                   f"🕐 آخرین خوندن کانال: {ch}\n"
                   f"⭐ استارز (کانال): {st}\n💎 پرمیوم (کانال): {pr}\n\n"
                   + "\n".join(f"{r['k']} = {r['v']}" for r in rows))

@ar.message(Command("setton"))
async def adm_setton(m: Message):
    p = m.text.split()
    if len(p) != 2: return await m.answer("استفاده: /setton 430000 (قیمت بازار هر تون به تومان؛ برای حالت خودکار 0)")
    try: v = float(norm(p[1]))
    except Exception: return await m.answer("عدد معتبر وارد کنید.")
    if v != 0 and not (10000 <= v <= 10000000): return await m.answer("عدد غیرمنطقیه.")
    await execute("UPDATE settings SET v=? WHERE k='ton_toman_manual'", (str(v),))
    await m.answer("✅ ثبت شد." if v else "✅ قیمت تون دوباره خودکار شد.")

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

@ar.message(Command("creditrial"))
async def adm_credit_rial(m: Message):
    p = m.text.split()
    if len(p) != 3: return await m.answer("استفاده: /creditrial آیدی_کاربر مبلغ_تومان")
    v = int(float(norm(p[2])))
    await execute("UPDATE users SET toman_balance=toman_balance+? WHERE id=?", (v, int(p[1])))
    await to_user(int(p[1]), f"✅ مبلغ {fm(v)} تومان به موجودی ریالی شما اضافه شد."); await m.answer("✅")

@ar.message(Command("creditdisc"))
async def adm_credit_disc(m: Message):
    p = m.text.split()
    if len(p) != 3: return await m.answer("استفاده: /creditdisc آیدی_کاربر مبلغ_تومان")
    v = int(float(norm(p[2])))
    await execute("UPDATE users SET discount_balance=discount_balance+? WHERE id=?", (v, int(p[1])))
    await execute("INSERT INTO discounts(user_id,amount,reason,created) VALUES(?,?,?,?)", (int(p[1]), v, "هدیه از طرف فروشگاه", now()))
    await to_user(int(p[1]), f"🎁 مبلغ {fm(v)} تومان تخفیف به حساب شما اضافه شد."); await m.answer("✅")

BUILTIN = {"ton": "تون", "stars": "استارز", "prem3": "پرمیوم ۳ ماهه", "prem6": "پرمیوم ۶ ماهه", "prem12": "پرمیوم ۱۲ ماهه"}

async def apply_sale(t, pct):
    """تخفیف ویژه روی قیمت. t: ton / stars / prem / all / شماره محصول. خروجی False = هدف نامعتبر"""
    if t == "all":
        for n in ("ton", "stars", "prem"): await execute("UPDATE settings SET v=? WHERE k=?", (str(pct), f"sale_pct_{n}"))
        await execute("UPDATE products SET sale_pct=?", (pct,))
    elif t in ("ton", "stars", "prem"):
        await execute("UPDATE settings SET v=? WHERE k=?", (str(pct), f"sale_pct_{t}"))
    elif t.isdigit():
        _, rc = await execute("UPDATE products SET sale_pct=? WHERE id=?", (pct, int(t)))
        if not rc: return False
    else:
        return False
    return True

@ar.message(Command("sale"))
async def adm_sale(m: Message):
    p = m.text.split()
    if len(p) != 3:
        rows = [f"{n}: {await sget('sale_pct_' + n)}٪" for n in ("ton", "stars", "prem")]
        cust = [f"#{r['id']} {r['title']}: {r['sale_pct']:g}٪" for r in await fetchall("SELECT * FROM products WHERE sale_pct>0")]
        return await m.answer("🔥 تخفیف‌های فعال:\n" + "\n".join(rows + cust) +
                              "\n\nاستفاده: /sale ton|stars|prem|all|شماره_محصول درصد (۰ = خاموش)\nمثال: /sale ton 3")
    try: pct = float(norm(p[2]))
    except Exception: return await m.answer("درصد معتبر وارد کنید.")
    if not (0 <= pct <= 50): return await m.answer("درصد باید بین ۰ تا ۵۰ باشد.")
    if not await apply_sale(p[1].lower(), pct): return await m.answer("هدف نامعتبر. ton / stars / prem / all / شماره محصول")
    await m.answer(f"✅ تخفیف ویژه {p[1]}: {pct:g}٪" if pct else f"✅ تخفیف {p[1]} خاموش شد.")

@ar.message(Command("addproduct"))
async def adm_addproduct(m: Message):
    p = m.text.split(maxsplit=3)
    if len(p) < 4 or p[1] not in ("prem", "other") or not norm(p[2]).isdigit():
        return await m.answer("استفاده:\n/addproduct prem|other قیمت_تومان عنوان\n/addproduct other 150000 notarget عنوان\n\n"
                              "prem = داخل منوی پرمیوم، other = دکمه‌ی «سایر محصولات»\nnotarget = یوزرنیم گیرنده پرسیده نشود")
    cat, price, rest = p[1], int(norm(p[2])), p[3]
    needs = 1
    if rest.startswith("notarget "): needs, rest = 0, rest[len("notarget "):]
    pid, _ = await execute("INSERT INTO products(category,title,price_toman,needs_target,active,created) VALUES(?,?,?,?,1,?)",
                           (cat, rest.strip(), price, needs, now()))
    await m.answer(f"✅ محصول #{pid} اضافه شد: {esc(rest)} — {fm(price)} تومان ({'منوی پرمیوم' if cat == 'prem' else 'سایر محصولات'})")

@ar.message(Command("products"))
async def adm_products(m: Message):
    lines = []
    for k, n in BUILTIN.items():
        lines.append(f"{'⛔' if not await available(k) else '✅'} {k} — {n}")
    for r in await fetchall("SELECT * FROM products ORDER BY id"):
        lines.append(f"{'✅' if r['active'] else '⛔'} #{r['id']} [{r['category']}] {esc(r['title'])} — {fm(r['price_toman'])} تومان"
                     + (f" — تخفیف {r['sale_pct']:g}٪" if r["sale_pct"] else ""))
    await m.answer("📦 محصولات:\n" + "\n".join(lines) + "\n\n/addproduct /editprice /delproduct /off /on /sale")

@ar.message(Command("editprice"))
async def adm_editprice(m: Message):
    p = m.text.split()
    if len(p) != 3 or not p[1].isdigit() or not norm(p[2]).isdigit(): return await m.answer("استفاده: /editprice شماره_محصول قیمت_تومان")
    _, rc = await execute("UPDATE products SET price_toman=? WHERE id=?", (int(norm(p[2])), int(p[1])))
    await m.answer("✅ قیمت عوض شد." if rc else "محصولی با این شماره نیست.")

@ar.message(Command("delproduct"))
async def adm_delproduct(m: Message):
    p = m.text.split()
    if len(p) != 2 or not p[1].isdigit(): return await m.answer("استفاده: /delproduct شماره_محصول")
    _, rc = await execute("DELETE FROM products WHERE id=?", (int(p[1]),))
    await m.answer("🗑 حذف شد." if rc else "محصولی با این شماره نیست.")

async def _toggle(m: Message, off):
    p = m.text.split()
    if len(p) != 2: return await m.answer(f"استفاده: /{'off' if off else 'on'} ton|stars|prem3|prem6|prem12|شماره_محصول")
    k = p[1].lower()
    if k in BUILTIN:
        await execute("UPDATE settings SET v=? WHERE k=?", ("1" if off else "0", f"off_{k}"))
    elif k.isdigit():
        _, rc = await execute("UPDATE products SET active=? WHERE id=?", (0 if off else 1, int(k)))
        if not rc: return await m.answer("محصولی با این شماره نیست.")
    else: return await m.answer("نام نامعتبر. /products را ببینید.")
    await m.answer(f"⛔ {k} ناموجود شد." if off else f"✅ {k} فعال شد.")

@ar.message(Command("off"))
async def adm_off(m: Message): await _toggle(m, True)

@ar.message(Command("on"))
async def adm_on(m: Message): await _toggle(m, False)

@ar.message(Command("stats"))
async def adm_stats(m: Message):
    n = (await fetchone("SELECT COUNT(*) c FROM users"))["c"]
    pend = (await fetchone("SELECT COUNT(*) c FROM orders WHERE status='pending'"))["c"]
    tot = (await fetchone("SELECT COALESCE(SUM(toman),0) s FROM orders WHERE status='approved'"))["s"]
    bal = (await fetchone("SELECT COALESCE(SUM(ton_nano),0) s FROM users"))["s"]
    rial = (await fetchone("SELECT COALESCE(SUM(toman_balance),0) a, COALESCE(SUM(discount_balance),0) b FROM users"))
    await m.answer(f"👥 کاربران: {n}\n⏳ سفارش در انتظار: {pend}\n💰 مجموع فروش تاییدشده: {fm(tot)} تومان\n"
                   f"👛 مجموع موجودی کاربران: {bal / NANO:.4f} TON\n💵 موجودی ریالی کاربران: {fm(rial['a'])} تومان\n🎁 تخفیف‌های بدهکار: {fm(rial['b'])} تومان")

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

# ═════════ پنل مدیریت با دکمه (بدون دستور) ═════════
class AddCat(StatesGroup): title = State()
class AddProd(StatesGroup): title = State(); price = State(); target = State()
class EditPrice(StatesGroup): value = State()
class ProdSale(StatesGroup): value = State()
class SaleAll(StatesGroup): value = State()

def ikb(rows): return InlineKeyboardMarkup(inline_keyboard=rows)
def btn(t, d): return InlineKeyboardButton(text=t, callback_data=d)

async def show(msg, text, kb=None, edit=False):
    if edit:
        try: return await msg.edit_text(text, reply_markup=kb)
        except Exception: pass
    await msg.answer(text, reply_markup=kb)

async def cat_title(key):
    if key == "prem": return "💎 منوی پرمیوم"
    if key == "other": return "🛍 سایر محصولات"
    r = await fetchone("SELECT title FROM categories WHERE id=?", (int(key[1:]),)) if key[1:].isdigit() else None
    return r["title"] if r else key

async def cat_keys():
    return ["prem", "other"] + [f"c{r['id']}" for r in await fetchall("SELECT id FROM categories ORDER BY id")]

PANEL_KB = ikb([[btn("🗂 دکمه‌های منو و محصولات", "ap:cats")], [btn("📦 همه محصولات", "ap:prods")],
                [btn("⛔ موجودی تون/استارز/پرمیوم", "ap:stock")], [btn("🔥 تخفیف ویژه", "ap:sale")],
                [btn("🔄 بروزرسانی منوی کاربران", "ap:push")]])

@ar.message(F.text == "🎛 پنل مدیریت")
@ar.message(Command("panel"))
async def panel(m: Message, state: FSMContext):
    await state.clear(); await m.answer("🎛 پنل مدیریت", reply_markup=PANEL_KB)

@ar.callback_query(F.data == "ap:home")
async def ap_home(c: CallbackQuery, state: FSMContext):
    await state.clear(); await c.answer(); await show(c.message, "🎛 پنل مدیریت", PANEL_KB, edit=True)

# ── دسته‌ها ──
async def render_cats(msg, edit=True):
    rows = []
    for k in await cat_keys():
        n = (await fetchone("SELECT COUNT(*) c FROM products WHERE category=?", (k,)))["c"]
        hidden = ""
        if k.startswith("c"):
            r = await fetchone("SELECT active FROM categories WHERE id=?", (int(k[1:]),))
            hidden = "" if r and r["active"] else " ⛔"
        rows.append([btn(f"{await cat_title(k)}{hidden} ({n})", f"catv:{k}")])
    rows += [[btn("➕ دکمه‌ی جدید در منوی ربات", "cnew")], [btn("🏠 پنل", "ap:home")]]
    await show(msg, "🗂 دسته‌ها: هر دسته یک دکمه در منوی کاربران است (بعد از اولین محصول ظاهر می‌شود).\n"
                    "«منوی پرمیوم» داخل بخش پرمیوم نشان داده می‌شود.", ikb(rows), edit)

@ar.callback_query(F.data == "ap:cats")
async def ap_cats(c: CallbackQuery):
    await c.answer(); await render_cats(c.message)

@ar.callback_query(F.data == "cnew")
async def cnew(c: CallbackQuery, state: FSMContext):
    await state.set_state(AddCat.title); await c.answer()
    await c.message.answer("اسم دکمه را بفرستید (مثلا: 🎁 گیفت‌ها). همین اسم در منوی کاربران دیده می‌شود.\nانصراف: /cancel")

@ar.message(AddCat.title, F.text)
async def cnew_title(m: Message, state: FSMContext):
    t = m.text.strip()
    if not (2 <= len(t) <= 30): return await m.answer("اسم باید بین ۲ تا ۳۰ حرف باشد.")
    if t in RESERVED_TITLES or await fetchone("SELECT id FROM categories WHERE title=?", (t,)):
        return await m.answer("این اسم قبلا استفاده شده، اسم دیگری بفرستید.")
    cid, _ = await execute("INSERT INTO categories(title,active,created) VALUES(?,1,?)", (t, now()))
    await state.clear()
    await m.answer(f"✅ دسته «{esc(t)}» ساخته شد. حالا اولین محصولش را اضافه کنید (بعدش دکمه در منو ظاهر می‌شود):",
                   reply_markup=ikb([[btn("➕ افزودن محصول", f"pnew:c{cid}")], [btn("🗂 دسته‌ها", "ap:cats")]]))

async def render_cat(msg, key, edit=True):
    prods = await fetchall("SELECT * FROM products WHERE category=? ORDER BY id", (key,))
    rows = [[btn(f"{'✅' if p['active'] else '⛔'} {p['title']} — {fm(p['price_toman'])}", f"pv:{p['id']}")] for p in prods]
    rows.append([btn("➕ افزودن محصول", f"pnew:{key}")])
    if key.startswith("c") and key[1:].isdigit():
        cr = await fetchone("SELECT active FROM categories WHERE id=?", (int(key[1:]),))
        if cr:
            rows.append([btn("⛔ پنهان کردن دکمه" if cr["active"] else "✅ نمایش دکمه", f"ctog:{key[1:]}"),
                         btn("🗑 حذف دسته", f"cdel:{key[1:]}")])
    rows.append([btn("🔙 دسته‌ها", "ap:cats")])
    await show(msg, f"🗂 <b>{esc(await cat_title(key))}</b>\nتعداد محصولات: {len(prods)}", ikb(rows), edit)

@ar.callback_query(F.data.startswith("catv:"))
async def catv(c: CallbackQuery):
    await c.answer(); await render_cat(c.message, c.data.split(":", 1)[1])

@ar.callback_query(F.data.startswith("ctog:"))
async def ctog(c: CallbackQuery):
    cid = int(c.data.split(":")[1])
    await execute("UPDATE categories SET active=1-active WHERE id=?", (cid,))
    await c.answer("انجام شد"); await render_cat(c.message, f"c{cid}")

@ar.callback_query(F.data.startswith("cdel:"))
async def cdel(c: CallbackQuery):
    cid = int(c.data.split(":")[1])
    if (await fetchone("SELECT COUNT(*) c FROM products WHERE category=?", (f"c{cid}",)))["c"]:
        return await c.answer("اول محصولات این دسته را حذف کنید.", show_alert=True)
    await execute("DELETE FROM categories WHERE id=?", (cid,))
    await c.answer("🗑 حذف شد"); await render_cats(c.message)

# ── افزودن محصول ──
@ar.callback_query(F.data.startswith("pnew:"))
async def pnew(c: CallbackQuery, state: FSMContext):
    await state.set_state(AddProd.title); await state.update_data(cat=c.data.split(":", 1)[1]); await c.answer()
    await c.message.answer("اسم محصول را بفرستید (مثلا: گیفت ۵۰ ستاره‌ای).\nانصراف: /cancel")

@ar.message(AddProd.title, F.text)
async def pnew_title(m: Message, state: FSMContext):
    t = m.text.strip()
    if not (2 <= len(t) <= 60): return await m.answer("اسم باید بین ۲ تا ۶۰ حرف باشد.")
    await state.update_data(title=t); await state.set_state(AddProd.price)
    await m.answer("قیمت را به تومان بفرستید (فقط عدد):")

@ar.message(AddProd.price, F.text)
async def pnew_price(m: Message, state: FSMContext):
    v = norm(m.text)
    if not v.isdigit() or int(v) <= 0: return await m.answer("یک عدد معتبر (تومان) بفرستید.")
    await state.update_data(price=int(v)); await state.set_state(AddProd.target)
    await m.answer("یوزرنیم گیرنده از کاربر پرسیده شود؟", reply_markup=ikb([[btn("✅ بله", "pt:1"), btn("❌ نه", "pt:0")]]))

@ar.callback_query(AddProd.target, F.data.startswith("pt:"))
async def pnew_target(c: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    pid, _ = await execute("INSERT INTO products(category,title,price_toman,needs_target,active,created) VALUES(?,?,?,?,1,?)",
                           (d["cat"], d["title"], d["price"], int(c.data.split(":")[1]), now()))
    await state.clear(); await c.answer("✅ اضافه شد")
    await render_prod(c.message, pid, edit=False)

# ── مدیریت محصول ──
async def render_prod(msg, pid, edit=True):
    p = await fetchone("SELECT * FROM products WHERE id=?", (pid,))
    if not p: return await msg.answer("محصول پیدا نشد.")
    text = (f"📦 <b>#{p['id']} {esc(p['title'])}</b>\n💰 قیمت: {fm(p['price_toman'])} تومان\n"
            f"🗂 دسته: {esc(await cat_title(p['category']))}\n"
            f"👤 یوزرنیم گیرنده: {'می‌پرسد' if p['needs_target'] else 'نمی‌پرسد'}\n"
            f"🔥 تخفیف ویژه: {p['sale_pct'] or 0:g}٪\nوضعیت: {'✅ موجود' if p['active'] else '⛔ ناموجود'}")
    kb = ikb([[btn("💰 تغییر قیمت", f"pe:{pid}"), btn("🔥 تخفیف", f"ps:{pid}")],
              [btn("⛔ ناموجود کن" if p["active"] else "✅ موجود کن", f"pa:{pid}")],
              [btn("🗑 حذف", f"pd:{pid}")], [btn("🔙 برگشت", f"catv:{p['category']}")]])
    await show(msg, text, kb, edit)

@ar.callback_query(F.data.startswith("pv:"))
async def pv(c: CallbackQuery):
    await c.answer(); await render_prod(c.message, int(c.data.split(":")[1]))

@ar.callback_query(F.data.startswith("pa:"))
async def pa(c: CallbackQuery):
    pid = int(c.data.split(":")[1])
    await execute("UPDATE products SET active=1-active WHERE id=?", (pid,))
    await c.answer("انجام شد"); await render_prod(c.message, pid)

@ar.callback_query(F.data.startswith("pd:"))
async def pd(c: CallbackQuery):
    pid = int(c.data.split(":")[1]); await c.answer()
    await show(c.message, "مطمئنید این محصول حذف شود؟", ikb([[btn("🗑 بله، حذف شود", f"pdy:{pid}"), btn("❌ نه", f"pv:{pid}")]]), edit=True)

@ar.callback_query(F.data.startswith("pdy:"))
async def pdy(c: CallbackQuery):
    pid = int(c.data.split(":")[1])
    p = await fetchone("SELECT category FROM products WHERE id=?", (pid,))
    await execute("DELETE FROM products WHERE id=?", (pid,))
    await c.answer("🗑 حذف شد")
    await render_cat(c.message, p["category"]) if p else await render_cats(c.message)

@ar.callback_query(F.data.startswith("pe:"))
async def pe(c: CallbackQuery, state: FSMContext):
    await state.set_state(EditPrice.value); await state.update_data(pid=int(c.data.split(":")[1])); await c.answer()
    await c.message.answer("قیمت جدید را به تومان بفرستید (فقط عدد).\nانصراف: /cancel")

@ar.message(EditPrice.value, F.text)
async def pe_value(m: Message, state: FSMContext):
    v = norm(m.text)
    if not v.isdigit() or int(v) <= 0: return await m.answer("یک عدد معتبر (تومان) بفرستید.")
    pid = (await state.get_data())["pid"]
    await execute("UPDATE products SET price_toman=? WHERE id=?", (int(v), pid)); await state.clear()
    await render_prod(m, pid, edit=False)

@ar.callback_query(F.data.startswith("ps:"))
async def ps(c: CallbackQuery, state: FSMContext):
    await state.set_state(ProdSale.value); await state.update_data(pid=int(c.data.split(":")[1])); await c.answer()
    await c.message.answer("درصد تخفیف ویژه را بفرستید (۰ تا ۵۰؛ ۰ = خاموش).\nانصراف: /cancel")

@ar.message(ProdSale.value, F.text)
async def ps_value(m: Message, state: FSMContext):
    try: pct = float(norm(m.text))
    except Exception: return await m.answer("یک عدد معتبر بفرستید.")
    if not (0 <= pct <= 50): return await m.answer("درصد باید بین ۰ تا ۵۰ باشد.")
    pid = (await state.get_data())["pid"]
    await execute("UPDATE products SET sale_pct=? WHERE id=?", (pct, pid)); await state.clear()
    await render_prod(m, pid, edit=False)

# ── همه محصولات ──
@ar.callback_query(F.data == "ap:prods")
async def ap_prods(c: CallbackQuery):
    await c.answer()
    rows = [[btn(f"{'✅' if p['active'] else '⛔'} #{p['id']} {p['title']} — {fm(p['price_toman'])}", f"pv:{p['id']}")]
            for p in await fetchall("SELECT * FROM products ORDER BY id DESC LIMIT 60")]
    rows.append([btn("🏠 پنل", "ap:home")])
    await show(c.message, "📦 محصولات (برای مدیریت، یکی را بزنید):" if len(rows) > 1 else "هنوز محصولی اضافه نشده.", ikb(rows), edit=True)

# ── موجودی محصولات اصلی ──
async def render_stock(msg, edit=True):
    rows = [[btn(f"{'✅' if await available(k) else '⛔'} {n}", f"st:{k}")] for k, n in BUILTIN.items()]
    rows.append([btn("🏠 پنل", "ap:home")])
    await show(msg, "هر کدام را بزنید تا «موجود/ناموجود» شود:", ikb(rows), edit)

@ar.callback_query(F.data == "ap:stock")
async def ap_stock(c: CallbackQuery):
    await c.answer(); await render_stock(c.message)

@ar.callback_query(F.data.startswith("st:"))
async def st_toggle(c: CallbackQuery):
    k = c.data.split(":")[1]
    if k not in BUILTIN: return await c.answer()
    await execute("UPDATE settings SET v=? WHERE k=?", ("0" if not await available(k) else "1", f"off_{k}"))
    await c.answer("انجام شد"); await render_stock(c.message)

# ── تخفیف ویژه ──
@ar.callback_query(F.data == "ap:sale")
async def ap_sale(c: CallbackQuery):
    await c.answer()
    cur = ", ".join([f"{n} {await sget('sale_pct_' + n)}٪" for n in ("ton", "stars", "prem")])
    await show(c.message, f"🔥 تخفیف ویژه روی قیمت\nفعلی: {cur}\n\nروی کدام اعمال شود؟ (برای تخفیف یک محصول خاص، از مدیریت همان محصول استفاده کنید)",
               ikb([[btn("تون", "sl:ton"), btn("استارز", "sl:stars"), btn("پرمیوم", "sl:prem")],
                    [btn("همه محصولات", "sl:all")], [btn("🏠 پنل", "ap:home")]]), edit=True)

@ar.callback_query(F.data.startswith("sl:"))
async def sl_pick(c: CallbackQuery, state: FSMContext):
    await state.set_state(SaleAll.value); await state.update_data(t=c.data.split(":")[1]); await c.answer()
    await c.message.answer("درصد تخفیف را بفرستید (۰ تا ۵۰؛ ۰ = خاموش).\nانصراف: /cancel")

@ar.message(SaleAll.value, F.text)
async def sl_value(m: Message, state: FSMContext):
    try: pct = float(norm(m.text))
    except Exception: return await m.answer("یک عدد معتبر بفرستید.")
    if not (0 <= pct <= 50): return await m.answer("درصد باید بین ۰ تا ۵۰ باشد.")
    t = (await state.get_data())["t"]; await state.clear()
    await apply_sale(t, pct)
    await m.answer(f"✅ تخفیف ویژه {t}: {pct:g}٪" if pct else f"✅ تخفیف {t} خاموش شد.", reply_markup=PANEL_KB)

# ── ارسال منوی جدید به کاربران ──
@ar.callback_query(F.data == "ap:push")
async def ap_push(c: CallbackQuery):
    await c.answer("در حال ارسال...")
    kb = await main_menu(); ok = 0
    for r in await fetchall("SELECT id FROM users WHERE banned=0 AND phone IS NOT NULL"):
        try: await user_bot.send_message(r["id"], "🔄 منوی ربات به‌روز شد.", reply_markup=kb); ok += 1
        except Exception: pass
        await asyncio.sleep(0.05)
    await c.message.answer(f"✅ منوی جدید برای {ok} کاربر ارسال شد.")

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
        price_loop(), channel_loop(), frag_loop(), pub_price_loop(), expiry_loop(), backup_loop())

if __name__ == "__main__":
    asyncio.run(main())
