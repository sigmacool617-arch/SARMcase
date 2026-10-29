"""SARMcase backend: FastAPI + Postgres (Neon). Деплой на Render."""
import hashlib, hmac, json, os, random, re, time, urllib.parse
from contextlib import asynccontextmanager

import asyncpg, httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
CHANNEL = os.environ.get("CHANNEL", "@SarmCase")
ADMIN_IDS = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()}
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")
ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")   # например https://ник.github.io
CD_MS = 12 * 3600 * 1000
TG = f"https://api.telegram.org/bot{BOT_TOKEN}"
SECRET = hashlib.sha256(("admin" + BOT_TOKEN).encode()).digest()

# ===== Игровые данные (держи в одном виде с index.html) =====
GOD = ["Huchini Bombini", "Los pugitos frenkunitos", "Skibidi toilet", "sausage mouse", "Me not Me before coffee", "bird", "bird,Sing a song", "Evaidik", "los shashlikis", "rodomitsini golden fishini", "Yes noobini", "Nagiev"]
SEC = ["Smell Bebra", "Max", "Gas oven", "Sisaliki", "ZOV", "Shashlik", "Black Pencil", "Huggy Waggy", "42", "1000-7", "Meme Combinasione", "13", "52", "2026", "Lisharty", "TrollFace Mango", "Get Up,Get Up", "Legendary Combinasione", "Cyber pug", "Lorna", "calculator", "52 and 42", "Dont get up", "Mimi", "Pi and door"]
PR = [2, 10, 14, 19, 22, 29, 30, 32, 34, 38, 40, 40, 42, 42, 43, 44, 44, 44, 46, 47, 58, 59, 60, 60, 63, 64, 65, 65, 70, 72, 76, 80, 82, 93, 100, 107, 132]
ITEMS = {n: ("god" if i < len(GOD) else "sec", PR[i]) for i, n in enumerate(GOD + SEC)}

def coin(n, w): return {"coin": n, "w": w}
def meme(n, w): return {"n": n, "w": w}

CASES = {
    "free":     {"price": 0,  "cd": True,  "sub": False, "drops": [coin(2, 60), coin(3, 20), meme("sausage mouse", 10), meme("Max", 9), meme("ZOV", 1)]},
    "sub":      {"price": 0,  "cd": True,  "sub": True,  "drops": [coin(2, 45), coin(5, 25), meme("Los pugitos frenkunitos", 10), meme("bird", 10), meme("Smell Bebra", 6), meme("ZOV", 4)]},
    "basic":    {"price": 9,  "cd": False, "sub": False, "drops": [coin(4, 80), meme("Los pugitos frenkunitos", 15), meme("Skibidi toilet", 5)]},
    "lux":      {"price": 58, "cd": False, "sub": False, "drops": [meme("Evaidik", 50), meme("Nagiev", 20), meme("42", 20), meme("2026", 7), meme("Dont get up", 3)]},
    "kirusha":  {"price": 28, "cd": False, "sub": False, "drops": [meme("Me not Me before coffee", 50), meme("bird", 30), meme("rodomitsini golden fishini", 20)]},
    "lisharty": {"price": 50, "cd": False, "sub": False, "drops": [meme("Max", 40), meme("ZOV", 20), meme("Shashlik", 15), meme("13", 12), meme("Lisharty", 10), meme("Lorna", 3)]},
}

# ===== Проверка, что запрос пришёл из Telegram =====
def verify(init_data: str) -> dict:
    try:
        pairs = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True, strict_parsing=True))
        got = pairs.pop("hash")
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc = hmac.new(key, check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc, got) or time.time() - int(pairs["auth_date"]) > 86400:
            raise ValueError
        return json.loads(pairs["user"])
    except Exception:
        raise HTTPException(401, "bad_auth")

# ===== База =====
pool: asyncpg.Pool = None

@asynccontextmanager
async def lifespan(app):
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL.split("?")[0], ssl="require", min_size=1, max_size=5, statement_cache_size=0)
    async with pool.acquire() as c:
        await c.execute("""
            create table if not exists users(id bigint primary key, username text, name text, data jsonb not null default '{}'::jsonb);
            create index if not exists users_un on users(lower(username));
            create table if not exists promos(code text primary key, kind text not null, value int not null, case_id text);""")
    yield
    await pool.close()

def fresh(): return {"bal": 0, "inv": {}, "best": None, "cd": {}, "fo": {}, "red": [], "sub": 0}

async def auth(init_data: str) -> dict:
    user = verify(init_data)
    await pool.execute(
        "insert into users(id,username,name,data) values($1,$2,$3,$4::jsonb) on conflict(id) do update set username=$2, name=$3",
        user["id"], (user.get("username") or "").lower(), user.get("first_name", ""), json.dumps(fresh()))
    return user

@asynccontextmanager
async def locked(uid: int):
    """Берёт строку игрока под замок, отдаёт состояние, сохраняет после блока."""
    async with pool.acquire() as c, c.transaction():
        row = await c.fetchrow("select data from users where id=$1 for update", uid)
        st = {**fresh(), **json.loads(row["data"])}
        yield st
        await c.execute("update users set data=$2::jsonb where id=$1", uid, json.dumps(st))

async def is_sub(uid: int) -> bool:
    try:
        async with httpx.AsyncClient(timeout=10) as h:
            j = (await h.get(f"{TG}/getChatMember", params={"chat_id": CHANNEL, "user_id": uid})).json()
        if not j.get("ok"):
            print("getChatMember:", j.get("description"))
            return False
        return j["result"]["status"] in ("creator", "administrator", "member")
    except Exception as e:
        print("is_sub error:", e)
        return False

class Req(BaseModel):
    initData: str = ""
    case: str = ""
    item: str = ""
    count: int = 1
    code: str = ""
    password: str = ""
    token: str = ""
    kind: str = ""
    value: int = 0
    username: str = ""
    amount: int = 0

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=[ORIGIN], allow_methods=["GET", "POST"], allow_headers=["*"])

@app.get("/")
def root(): return {"ok": True}

@app.post("/api/me")
async def me(r: Req):
    user = await auth(r.initData)
    async with locked(user["id"]) as st: pass
    return {"user": {"id": user["id"], "name": user.get("first_name", ""), "username": user.get("username", "")}, "state": st}

@app.post("/api/sub")
async def sub(r: Req):
    user = await auth(r.initData)
    ok = await is_sub(user["id"])
    async with locked(user["id"]) as st:
        st["sub"] = 1 if ok else 0
    return {"ok": ok, "state": st}

@app.post("/api/open")
async def open_case(r: Req):
    user = await auth(r.initData); uid = user["id"]
    case = CASES.get(r.case)
    if not case: raise HTTPException(400, "bad_case")
    if case["sub"] and not await is_sub(uid): raise HTTPException(403, "sub")
    async with locked(uid) as st:
        fo, now = st["fo"].get(r.case, 0), int(time.time() * 1000)
        if fo > 0:
            st["fo"][r.case] = fo - 1
        else:
            if case["cd"] and st["cd"].get(r.case, 0) > now: raise HTTPException(429, "cooldown")
            if st["bal"] < case["price"]: raise HTTPException(402, "no_money")
            st["bal"] -= case["price"]
            if case["cd"]: st["cd"][r.case] = now + CD_MS
        d = random.choices(case["drops"], weights=[x["w"] for x in case["drops"]])[0]
        if "coin" in d:
            st["bal"] += d["coin"]; win = {"coin": d["coin"]}
        else:
            n = d["n"]; rar, p = ITEMS[n]
            st["inv"][n] = st["inv"].get(n, 0) + 1
            if not st["best"] or p > st["best"]["p"]: st["best"] = {"n": n, "r": rar, "p": p}
            win = {"n": n}
        if case["sub"]: st["sub"] = 1
    return {"win": win, "state": st}

@app.post("/api/sell")
async def sell(r: Req):
    user = await auth(r.initData)
    if r.item not in ITEMS or r.count < 1: raise HTTPException(400, "no_item")
    async with locked(user["id"]) as st:
        have = st["inv"].get(r.item, 0)
        if have < r.count: raise HTTPException(400, "no_item")
        if have == r.count: del st["inv"][r.item]
        else: st["inv"][r.item] = have - r.count
        st["bal"] += ITEMS[r.item][1] * r.count
    return {"state": st}

@app.post("/api/promo")
async def promo(r: Req):
    user = await auth(r.initData)
    code = r.code.strip().upper()
    row = await pool.fetchrow("select kind,value,case_id from promos where code=$1", code)
    if not row: raise HTTPException(404, "bad_promo")
    async with locked(user["id"]) as st:
        if code in st["red"]: raise HTTPException(409, "used_promo")
        st["red"].append(code)
        if row["kind"] == "coins":
            st["bal"] += row["value"]; msg = f"Получено монет: {row['value']}"
        else:
            st["fo"][row["case_id"]] = st["fo"].get(row["case_id"], 0) + row["value"]; msg = f"Получено открытий: {row['value']}"
    return {"state": st, "msg": msg}

@app.get("/api/avatar")
async def avatar(initData: str = Query(...)):
    """Аватарка игрока: initData не всегда содержит photo_url, поэтому берём фото через бота."""
    user = verify(initData)
    async with httpx.AsyncClient(timeout=15) as h:
        j = (await h.get(f"{TG}/getUserProfilePhotos", params={"user_id": user["id"], "limit": 1})).json()
        photos = j.get("result", {}).get("photos", [])
        if not photos: raise HTTPException(404, "no_photo")
        sizes = photos[0]
        fid = sizes[min(1, len(sizes) - 1)]["file_id"]
        path = (await h.get(f"{TG}/getFile", params={"file_id": fid})).json()["result"]["file_path"]
        img = await h.get(f"https://api.telegram.org/file/bot{BOT_TOKEN}/{path}")
    return Response(img.content, media_type="image/jpeg", headers={"Cache-Control": "private, max-age=3600"})

# ===== Админка: Telegram-ID из ADMIN_IDS + пароль из ADMIN_PASSWORD =====
fails: dict = {}

def make_token(uid: int) -> str:
    m = f"{uid}.{int(time.time()) + 3600}"
    return f"{m}.{hmac.new(SECRET, m.encode(), hashlib.sha256).hexdigest()}"

def check_token(uid: int, t: str):
    try:
        u, e, s = t.split(".")
        ok = hmac.compare_digest(s, hmac.new(SECRET, f"{u}.{e}".encode(), hashlib.sha256).hexdigest()) and int(u) == uid and int(e) > time.time()
    except Exception:
        ok = False
    if not ok or uid not in ADMIN_IDS: raise HTTPException(403, "bad_admin")

async def admin(r: Req) -> dict:
    user = await auth(r.initData)
    check_token(user["id"], r.token)
    return user

@app.post("/api/admin/login")
async def admin_login(r: Req):
    user = await auth(r.initData); uid = user["id"]
    if uid not in ADMIN_IDS or not ADMIN_PASSWORD: raise HTTPException(403, "bad_admin")
    f = [t for t in fails.get(uid, []) if t > time.time() - 600]
    if len(f) >= 5: raise HTTPException(429, "locked")
    if not hmac.compare_digest(r.password.encode(), ADMIN_PASSWORD.encode()):
        fails[uid] = f + [time.time()]
        raise HTTPException(403, "bad_pass")
    return {"token": make_token(uid)}

@app.post("/api/admin/promo_create")
async def promo_create(r: Req):
    await admin(r)
    code = r.code.strip().upper()
    if not re.fullmatch(r"[A-Z0-9_-]{3,24}", code) or r.value < 1 or r.kind not in ("coins", "case") or (r.kind == "case" and r.case not in CASES):
        raise HTTPException(400, "bad_data")
    await pool.execute("insert into promos(code,kind,value,case_id) values($1,$2,$3,$4) on conflict(code) do update set kind=$2,value=$3,case_id=$4",
                       code, r.kind, r.value, r.case if r.kind == "case" else None)
    return {"ok": True}

@app.post("/api/admin/promo_delete")
async def promo_delete(r: Req):
    await admin(r)
    await pool.execute("delete from promos where code=$1", r.code.strip().upper())
    return {"ok": True}

@app.post("/api/admin/promo_list")
async def promo_list(r: Req):
    await admin(r)
    rows = await pool.fetch("select code,kind,value,case_id from promos order by code")
    return {"promos": [{"code": x["code"], "kind": x["kind"], "value": x["value"], "case": x["case_id"]} for x in rows]}

@app.post("/api/admin/give")
async def give(r: Req):
    user = await admin(r)
    un = r.username.strip().lstrip("@").lower()
    row = await pool.fetchrow("select id from users where lower(username)=$1", un) if un else None
    if not row or not (1 <= r.amount <= 1_000_000_000): raise HTTPException(404, "no_user")
    async with locked(row["id"]) as st:
        st["bal"] += r.amount
    return {"ok": True, "state": st if row["id"] == user["id"] else None}
