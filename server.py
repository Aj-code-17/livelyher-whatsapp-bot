"""Livelyher — TESTING BUILD (do NOT deploy to production).

Identical to server.py EXCEPT: the 30-minute analysis timer is bypassed.
Intake end -> MSG_END -> about video -> ~12s -> PITCH_1 -> stage 6 instantly.
Everything else is v16-identical: link previews on (thumbnail cards),
bare .mp4/media-id ships as native video, parked-ack silence, tightened
refusal, intake sanity check, ack-hold silence + anti-repeat, updated
intake wording, Message 12 with 24-hr delivery + printed sentence,
5s/3s video pacing, sliding debounce with stall cap, pure-ack fast path,
3s price-scarcity pair, repetition-aware holds with steer-back, official
master-script messages, full FAQ base, AI stage-1 bridge, opt-out parking,
typing indicator, resume-not-repeat bursts, barge-in abort, reply-lag
intent, pause handling, pacing tiers, audio-note reply, disk persistence.

Use:
    uvicorn server_testing:app --host 0.0.0.0 --port 8000
Switch back to the real 30-minute timer by running server.py instead.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import random
import re
import sqlite3
import time
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from groq import Groq

from bot.messenger import MetaClient

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
log = logging.getLogger("server")

# ------------------------------------------------------------ configuration
meta = MetaClient(
    page_access_token=os.getenv("WA_ACCESS_TOKEN", ""),
    app_secret=os.getenv("META_APP_SECRET", ""),
)
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "dev-verify-token")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID", "")
DB_PATH = os.getenv("DB_PATH", "conversations.db")

GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
ANALYSIS_DELAY_MINUTES = float(os.getenv("ANALYSIS_DELAY_MINUTES", "30"))
# Sliding quiet-window debounce (the closest thing to typing-detection the
# WhatsApp API allows — Meta never sends "user is typing" events to bots):
# every new message RE-ARMS the clock, so we only answer after she has been
# quiet for DEBOUNCE_QUIET seconds. If she types non-stop for
# DEBOUNCE_STALL_MAX seconds, we answer what we have so she can't stall us.
DEBOUNCE_QUIET = float(os.getenv("DEBOUNCE_QUIET_SECONDS", "18"))
DEBOUNCE_STALL_MAX = float(os.getenv("DEBOUNCE_STALL_MAX_SECONDS", "90"))
DEBOUNCE_POLL = float(os.getenv("DEBOUNCE_POLL_SECONDS", "0.5"))

_groq_client = None


def groq() -> Groq:
    """Lazy client — a missing key must never crash the app at import."""
    global _groq_client
    if _groq_client is None:
        key = os.getenv("GROQ_API_KEY")
        if not key:
            raise RuntimeError("GROQ_API_KEY is not set in the environment!")
        _groq_client = Groq(api_key=key)
    return _groq_client


def _clean(text: str) -> str:
    """Sanitize AI output: no markdown symbols and NO hyphens/dashes of any
    kind, per client rule. Applied to everything the LLM produces."""
    t = (text or "").strip()
    for ch in ("*", "#", "-", "–", "—", "_"):
        t = t.replace(ch, " ")
    t = re.sub(r"[ \t]{2,}", " ", t)
    return t.strip()


# ------------------------------------------------------------ conversation templates
MSG_1 = "Asslamualikum! it's Ani from livelyher, how are you Ma'am?"
MSG_2 = "great I will ask you some basic questions, then we will analyse your situation and reach out to you in 30 minutes where we will explain your situation in detail and how we will help you fix it, Inshallah! And Ma'am please reply in text not voice messages…"
SET_1 = "Kindly tell us about:\n1. Aapka Current Weight aur Target Weight (kg) kitna hai, aur aapki Height kya hai?\n2. Ye weight gain kab shuru hua, shaadi ke baad, pregnancy/delivery ke baad, ya pichle 1–2 saalon mein achanak barha?\n3. Body mein stubborn weight sabse zyada kahan mehsoos hota hai — lower belly/stomach fat, hips/thighs, ya overall heavy bloating?"
SET_2 = "4. Pehle weight loss ke liye kya try kiya hai, crash diet, green teas, meal skipping, ya intermittent fasting and usei faida kyun hua?\n5. Aapki daily eating routine kaisi rehti hai, exactly what you usually eat in breakfast, lunch, dinner and snacking? iska answer thora detail mei dijye ga also tell the timing when you eat\n6. Kya koi hormonal blocker ya issue hai jiski wajah se weight drop nahi hota (jaise PCOS, Thyroid, ya irregular cycles)?"
SET_3 = "For Understanding your Mood and Stress Level:\n1. 1 se 10 ke scale par aap apna daily anxiety aur mental stress kis number par rank karengi (jahan 1 ka matlab bilkul calm aur 10 ka matlab extreme overthinking ya bechaini ho)?\n2. Aapki sleep routine kaisi rehti hai, kya raat ko sote waqt mind switch off nahi hota ya neend toot-toot kar aati hai, aur subah uthne par energy bilkul low hoti hai?\n3. Aapko stress ya anxiety feel hoti hai? Ya aise lage kei jin cheezun ki pehlay enjoy krte that wo ab achi nai lagtin? Ya choti choti baat per gussa ya irritability hoti hoo?"
MSG_END = "Thanks for sharing information Mam, we will analyse your situation and reach out to you in about 30 minutes\n\nWe will explain you in detail for FREE your issue, why it is happening and how we can help you and only once you are satisfied you can buy your Personalized plan, which will be delivered to you in 24hrs! 😇"
ABOUT_VIDEO = "In the meantime, please watch this video to know more about us: https://www.instagram.com/reel/DeKqR4Uuw6D/?stkn=MWZtZ3R4OGE2ZGljdQ=="  # <--- ADD YOUR ABOUT VIDEO LINK HERE
MSG_WAIT = ("Perfect Ma'am! 😊 Our coaches are analysing your answers right now — "
            "we'll reach out to you shortly, Inshallah.")
INVALID_FALLBACK = ("Ma'am, could you please answer the questions above? "
                    "They help our coaches understand your situation properly.")
HESITATION_FALLBACK = ("No problem Ma'am 😊 take your time, I am right here whenever "
                       "you are ready to continue.")
MSG_CONFIRM_PAYMENT = ("Perfect Ma'am! 🎉 Once you have made the payment, just share "
                       "the screenshot here and we will confirm your spot right away, "
                       "Insha'Allah.")
MSG_SCREENSHOT = ("JazakAllah Ma'am! 🌸 We have received your screenshot. Our team is "
                  "verifying the payment and will Insha'Allah confirm your spot shortly.")
MSG_TYPE_ONLY = ("Sorry Ma'am, I can understand text messages only. Kindly type your "
                 "reply here and I will help you right away 😊")
MSG_AUDIO = "Ma'am I can't listen to audio messages, kindly reply in text messages 😊"

# Opt-out flow: she said no / stop / not interested — acknowledge ONCE, park
# her, never re-ask. If she ever returns, resume from resume_stage.
MSG_GOODBYE = ("No problem at all Ma'am 🌸 Thank you for your time. If you ever "
               "change your mind, just message me here and we can pick up right "
               "where you left off.")
MSG_OPTOUT_FINAL = "Of course Ma'am, take care 🌸"
MSG_WELCOME_BACK = "Welcome back Ma'am! 😊 Continuing right from where you left off."
MSG_PAUSE = "Sure Ma'am, take your time. I am right here whenever you are ready."
STAGE_OPTED_OUT = 50        # goodbye sent; one final soft line allowed
STAGE_OPTED_OUT_HARD = 51   # final line sent; stay respectfully silent unless she re-engages

# ---------------- FAQ KNOWLEDGE BASE (EDIT THIS WITH YOUR REAL INFO)
# The AI answers her questions ONLY from these facts, so keep it accurate.
# Used by: opening analysis (hi/hello stage), stage-1 bridge, intake
# validator, and the funnel intent checker.
FAQ_KNOWLEDGE_BASE = """livelyher is an online weight loss and wellness coaching program for women.
- Process: no payment first. Her details are collected and a FREE personalized analysis is done and explained. Only once she is fully satisfied does she choose to buy her personalized plan.
- Intake details collected: current weight, target weight, height, eating routine, stress and sleep, any hormonal issues; a few extra diet preference questions may be asked for the final plan.
- Pricing: one-time purchase, NO monthly subscription, no hidden charges. Digital plan: 1470 PKR (51 percent off the original 3000). Printed plan: 1950 PKR plus 200 delivery charges, and the content is exactly the same as the digital plan, only the format differs.
- Payment: advance payment is preferred. Digital plans are paid via JazzCash, EasyPaisa, or direct bank transfer. Printed plans offer Cash on Delivery (COD). If she has a genuine problem paying in advance, she can pay after receiving the plan as well.
- Delivery: digital plan within 24 hours of purchase. Printed plan to the physical address within 5 to 7 working days. The plan is a clean, easy-to-read visual manual with clear text and simple icons.
- Customization: fully customized to her condition and food preferences (PCOS, thyroid, vegetarian, anything). Meals use everyday affordable home foods like daal, tawa cooked chicken and shami kebabs with portion control, nothing fancy to buy.
- Results: no fixed number is guaranteed because every body reacts differently, but if she follows the plan and still sees no results, a brand new adjusted plan is made for her entirely free. Heavy gym is not required; a simple 15 minute daily walk is recommended.
- Support: 4 weeks of WhatsApp support starting the day she receives the manual (meal swaps, motivation, guidance). Support can be extended completely free just by sharing a review of the experience.
- Team and location: main operations are based in Gujrat, but LivelyHer works mainly as a virtual team serving clients fully online, so no clinic or office visit is ever needed. Plans are created by a professional network of multiple dieticians and psychologists.
- Voice notes: she should kindly reply in text messages, because voice messages cannot be heard.
- If asked something not covered here, say politely that the team will confirm it right after the free analysis. Do not invent facts."""

# ---------------- OFFICIAL SCRIPT (Solution Explain, Messages 1..16)
PITCH_1 = "Asslamualaikum... we are done with the analysis, let me know when you are there Ma'am?"  # Message 1: as it is, wait for reply
PITCH_3 = "are you getting my point?"                                                              # Message 3: as it is, wait for reply
PITCH_5 = "So we are setting a goal for you...we have to lose 6 to 7 kg weight in coming 6 weeks aur specially stress aur anxiety ko bilkul khatam krna hai because uskei bagair weight loss mushkil hota aur specially for women mood fresh aur lively hona wese hi bohat zaroori hai"  # Message 5: KEEP as it is
PITCH_6_TEMPLATE = "For that, I will make just few changes in your diet and recommend few vitamins and a tea, this will {AI_EXPLAIN} and also, we will create a mood plan for you, Insha'Allah, it will help you a lot with mood and energy"  # Message 6: structure kept, AI fills the explain slot
PITCH_7 = "And I am confident kei Insha'Allah in next 6 weeks we can achieve these results because first because we will design it exactly according to your routine you described so it will be very easy for you to follow and also, we will always be available to you whenever you need any help😇"  # Message 7: KEEP as it is
PITCH_8 = "I am sharing the review video of one of our client so you better know how it is... they ordered a printed version…"  # Message 8: wait for response before next
PITCH_9 = "https://www.instagram.com/reel/DeKshjLOHyg/?stkn=azNhbjV1Nm56eW1h"  # Message 10 = VIDEO  <--- ADD YOUR VIDEO LINK HERE
PITCH_10 = "let me know once you have seen it, I will share more details than .."        # Message 11: wait for reply
PITCH_11 = "The original price is 3000 it's on 51% discount for this so it will be 1470 only...aur for 4 weeks I will be there to support for any changes insha'Allah. We will create it in 24 hrs and send to you on here but if you want printed delivered to your home, we can also do that with printing and delivery charges added😊"  # Message 12: keep as it is
PITCH_13 = "Also Mam there are only 7 spots left in this batch aur aaj close hojaye ga….hum nei bohat detailed aur time laga ker analysis already krlia hai....lekin abhi kuch questions aur puchne hain regarding your diet preferences for making final plan...should I send you the questions?"  # Message 13: wait for reply
PITCH_15 = "Okay I will send you the questions aapko within 24 hrs plan miljay ga insha'Allah mei questions bana ker kuch deir mei bhejti hun..."  # Message 14: keep as it is
PITCH_16 = ("I will send you the questions from the number 03096015390. It's for our close "
            "customers and also for any questions, you have to contact on this number 😊\n\n"
            "For payment you can use following accounts:\n\n"
            "Bank: [BANK NAME]\nAccount: [ACCOUNT NUMBER]\nTitle: Livelyher")  # Message 15  <--- ADD CONTACT NUMBER + BANK DETAILS HERE


# ------------------------------------------------------------ database
def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def setup_database() -> None:
    conn = _db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_phone TEXT PRIMARY KEY,
            bot_phone_id TEXT,
            chat_stage INTEGER DEFAULT 0,
            answers_1 TEXT,
            answers_2 TEXT,
            answers_3 TEXT,
            analysis_send_at TEXT,
            analysis_text TEXT          -- pre-generated analysis, stored at intake
        );
        CREATE TABLE IF NOT EXISTS seen_messages (message_id TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS chat_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_phone TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            ts REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_history_phone ON chat_history(user_phone, id);
    """)
    # Migration for databases created before analysis_text existed:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "analysis_text" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN analysis_text TEXT")
    if "resume_stage" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN resume_stage INTEGER DEFAULT 0")
    conn.commit()
    conn.close()


# ------------------------------------------------------- chat history helpers
def add_history(user_phone: str, role: str, content: str) -> None:
    with _db() as conn:
        conn.execute(
            "INSERT INTO chat_history(user_phone, role, content, ts) VALUES (?,?,?,?)",
            (user_phone, role, content, time.time()),
        )


def get_history(user_phone: str, limit: int = 10) -> list[dict]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT role, content FROM chat_history WHERE user_phone=? "
            "ORDER BY id DESC LIMIT ?", (user_phone, limit),
        ).fetchall()
    return [{"role": r, "content": c} for r, c in reversed(rows)]


def _history_block(user_phone: str, limit: int = 10) -> str:
    rows = get_history(user_phone, limit)
    if not rows:
        return "(no earlier conversation)"
    return "\n".join(f"{'user' if r['role'] == 'user' else 'bot'}: {r['content']}" for r in rows)


def _utcnow_plus(minutes: float) -> str:
    return (datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(minutes=minutes)).isoformat()


# ------------------------------------------------- 30-minute analysis delivery
def _ensure_analysis(phone: str, bot_id: str) -> None:
    """Send PITCH_1 to one lead whose analysis came due; marks stage 6 only
    AFTER a successful send so failures are retried next tick / next boot."""
    with _db() as conn:
        row = conn.execute(
            "SELECT answers_1, answers_2, answers_3, analysis_text "
            "FROM users WHERE user_phone=?", (phone,)).fetchone()
    if not row:
        return
    a1, a2, a3, stored = row

    if not stored:
        # LLM was unreachable at intake time — generate it now, lazily.
        stored = _safe_medical_pitch(a1, a2, a3)

    meta.send_whatsapp_text(bot_id or PHONE_NUMBER_ID, phone, PITCH_1)
    add_history(phone, "bot", PITCH_1)
    log.info("[WA] -> %s: 30-minute analysis doorbell sent", phone)

    with _db() as conn:
        conn.execute(
            "UPDATE users SET chat_stage=6, analysis_text=COALESCE(analysis_text, ?) "
            "WHERE user_phone=?", (stored, phone))
        conn.commit()


def check_scheduled_analyses() -> None:
    """Runs every 60s AND once at every server boot: delivers every analysis
    that is due but was never sent — this is the wake-up verification step."""
    conn = None
    try:
        conn = _db()
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        due = conn.execute(
            "SELECT user_phone, bot_phone_id FROM users "
            "WHERE chat_stage = 5 AND analysis_send_at <= ?", (now,)).fetchall()
        conn.close()
        conn = None

        for phone, bot_id in due:
            try:
                _ensure_analysis(phone, bot_id)
            except Exception:
                # Don't update the stage — next tick/boot will try again.
                log.exception("Analysis delivery failed for %s (will retry)", phone)
    except Exception:
        log.exception("check_scheduled_analyses failed")
    finally:
        if conn:
            conn.close()


_scheduler: BackgroundScheduler | None = None


def _start_background() -> None:
    global _scheduler
    setup_database()
    if _scheduler is None:
        _scheduler = BackgroundScheduler()
        _scheduler.add_job(check_scheduled_analyses, "interval", seconds=60)
        _scheduler.start()
        # WAKE-UP SWEEP: on every boot, immediately check for overdue analyses
        # (covers server sleep, restarts, deploys, crashes).
        _scheduler.add_job(check_scheduled_analyses)
    with _db() as conn:
        leads = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        rows = conn.execute("SELECT COUNT(*) FROM chat_history").fetchone()[0]
    log.info("State restored from disk: %d leads, %d history rows (restart & update safe)",
             leads, rows)
    log.info("Livelyher started (model=%s, delay=%s min) — scheduler + boot sweep armed",
             GROQ_MODEL, ANALYSIS_DELAY_MINUTES)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _start_background()
    yield
    if _scheduler:
        _scheduler.shutdown()


app = FastAPI(title="Livelyher WhatsApp Bot — TESTING (no 30-min wait)", lifespan=lifespan)
_start_background()


# ---------------------------------------------------------------- endpoints
@app.get("/")
def root():
    return {"status": "running", "bot": "livelyher", "model": GROQ_MODEL}


@app.get("/webhook")
def verify_webhook(request: Request) -> Response:
    q = request.query_params
    if q.get("hub.mode") == "subscribe" and q.get("hub.verify_token") == VERIFY_TOKEN:
        log.info("Webhook verified by Meta ✅")
        return Response(content=q.get("hub.challenge", ""), media_type="text/plain")
    log.warning("Webhook verification failed (wrong token?) params=%s", dict(q))
    return Response(status_code=403)


# ------------------------------------------------- sliding debounce machinery
_buffers: dict[str, list[tuple[str, str]]] = {}  # (message_id, text) per lead
_workers: dict[str, asyncio.Task] = {}
_interrupted: set[str] = set()  # leads who spoke while we were mid-reply
_windows: dict[str, tuple[float, float]] = {}  # phone -> (first_at, latest_at) monotonic


class _BurstAborted(Exception):
    """The lead sent a new message while a multi-message burst was in flight."""


# ------------------------------------------------------------------ Reply pacing
# Information-collection (intake Q&A): quick 4s. Short messages: max 7s.
# Big sections: 12s. All env-tunable.
INTAKE_GAP = float(os.getenv("INTAKE_GAP", "4"))
SHORT_GAP = float(os.getenv("SHORT_GAP", "7"))
BIG_GAP = float(os.getenv("BIG_GAP", "12"))
PAIR_GAP = float(os.getenv("PAIR_GAP", "3"))   # gap between tightly-linked pairs (price -> scarcity)
VIDEO_PRE_GAP = float(os.getenv("VIDEO_PRE_GAP", "5"))     # after Message 8, before the review video
VIDEO_POST_GAP = float(os.getenv("VIDEO_POST_GAP", "3"))   # after the review video, before Message 11
BIG_MESSAGE_MIN = 200  # chars; messages this long or longer count as "big"


async def _auto_gap(text: str) -> None:
    """Typing-realistic pause sized by the message about to be sent."""
    await asyncio.sleep(BIG_GAP if len(text) >= BIG_MESSAGE_MIN else SHORT_GAP)


_gen_cache: dict[tuple[str, str], str] = {}  # (phone, role) -> generated text


def _already_sent(sender_phone: str, text: str) -> bool:
    """True if this exact bot message is already in her persistent history."""
    with _db() as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM chat_history WHERE user_phone = ? AND role = 'bot'"
            " AND content = ?", (sender_phone, text)).fetchone()[0]
    return n > 0


def _last_bot_message(sender_phone: str) -> str:
    """The most recent bot message she received (empty string if none)."""
    row = _db().execute(
        "SELECT content FROM chat_history WHERE user_phone = ? AND role = 'bot'"
        " ORDER BY rowid DESC LIMIT 1", (sender_phone,)).fetchone()
    return row[0] if row else ""


def _is_video_payload(text: str) -> bool:
    """True if the message is ONLY a video file link (https://... .mp4) or a
    WhatsApp media id ('media:<id>') — those ship as native video messages.
    A sentence containing a link (like the about video) stays text and gets
    the link-preview card instead."""
    t = text.strip().lower()
    bare_url = t.startswith("http") and " " not in t
    return (bare_url and t.endswith((".mp4", ".mov", ".webm"))) or t.startswith("media:")


def _seen_or_mark(message_id: str) -> bool:
    if not message_id:
        return False
    with _db() as conn:
        cur = conn.execute("INSERT OR IGNORE INTO seen_messages(message_id) VALUES (?)",
                           (message_id,))
        return cur.rowcount == 0


def _enqueue(sender_phone: str, bot_phone_id: str, message_id: str, message_text: str) -> None:
    if _seen_or_mark(message_id):
        log.info("Duplicate delivery of %s — skipping", message_id)
        return
    buf = _buffers.setdefault(sender_phone, [])
    now = time.monotonic()
    if not buf:
        _windows[sender_phone] = (now, now)      # fresh turn window
    else:
        _windows[sender_phone] = (_windows[sender_phone][0], now)  # RE-ARM the clock
    buf.append((message_id, message_text))
    if message_text.strip():
        _interrupted.add(sender_phone)  # fresh input: abort any in-flight burst ASAP
    worker = _workers.get(sender_phone)
    if worker is None or worker.done():
        task = asyncio.create_task(_conversation_worker(sender_phone, bot_phone_id))
        task.add_done_callback(_log_task_result)
        _workers[sender_phone] = task


async def _conversation_worker(sender_phone: str, bot_phone_id: str) -> None:
    """Sliding debounce per lead: every message she sends re-arms the quiet
    clock — we only process her turn after DEBOUNCE_QUIET seconds of silence
    (the API-safe equivalent of "wait while she's typing"), combining
    everything into ONE reply so there are never double replies. If she types
    non-stop for DEBOUNCE_STALL_MAX seconds, we answer what we have anyway."""
    try:
        while True:
            await asyncio.sleep(DEBOUNCE_POLL)
            buf = _buffers.get(sender_phone)
            if not buf:
                break
            now = time.monotonic()
            first_at, latest_at = _windows.get(sender_phone, (now, now))
            quiet_for = now - latest_at
            waiting_for = now - first_at
            if quiet_for < DEBOUNCE_QUIET and waiting_for < DEBOUNCE_STALL_MAX:
                continue  # she re-armed the clock — she's still typing, keep waiting
            items = _buffers.pop(sender_phone, [])
            _windows.pop(sender_phone, None)
            if quiet_for < DEBOUNCE_QUIET:
                log.info("--> [STALL-CAP] %s typed non-stop for %.0fs — answering now",
                         sender_phone, DEBOUNCE_STALL_MAX)
            combined = "\n".join(t for _mid, t in items if t).strip()
            if not combined:
                break
            if len(items) > 1:
                log.info("--> [BATCHED] %d messages from %s combined into one reply",
                         len(items), sender_phone)
            # Show 'typing...' on her phone while the reply is being composed.
            last_mid = next((m for m, t in reversed(items) if m), "")
            if last_mid:
                try:
                    await asyncio.to_thread(meta.send_typing_indicator,
                                            bot_phone_id, last_mid)
                except Exception:
                    log.debug("typing indicator failed (cosmetic)", exc_info=True)
            add_history(sender_phone, "user", combined)
            await _dispatch_safe(sender_phone, bot_phone_id, combined)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Conversation worker crashed for %s", sender_phone)
    finally:
        _workers.pop(sender_phone, None)
        _windows.pop(sender_phone, None)


@app.post("/webhook", response_model=None)
async def receive_webhook(request: Request):
    body = await request.body()
    log.info("POST /webhook (%d bytes) from %s",
             len(body), request.client.host if request.client else "?")

    if not meta.verify_signature(body, request.headers.get("X-Hub-Signature-256")):
        log.warning("Rejected POST with invalid signature")
        return Response(status_code=403)

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        log.warning("POST with invalid JSON: %.200s", body)
        return {"status": "ignored (bad json)"}

    if data.get("object") != "whatsapp_business_account":
        return {"status": "ignored (not whatsapp)"}

    for entry in data.get("entry", []):
        for change in entry.get("changes", []):
            val = change.get("value", {})
            bot_phone_id = (val.get("metadata") or {}).get("phone_number_id") or PHONE_NUMBER_ID

            for msg in val.get("messages", []):
                if msg.get("type") != "text":
                    # Never leave a voice note / image / document on "seen":
                    # acknowledge it instead of going silent.
                    if _seen_or_mark(msg.get("id", "")):
                        continue
                    log.info("Non-text message type=%s from %s — ack queued",
                             msg.get("type"), msg.get("from"))
                    task = asyncio.create_task(_ack_nontext(
                        msg.get("from"), bot_phone_id, msg.get("type")))
                    task.add_done_callback(_log_task_result)
                    continue

                sender_phone = msg.get("from")
                message_id = msg.get("id", "")
                message_text = (msg.get("text") or {}).get("body", "").strip()
                log.info("[WA] %s: %s", sender_phone, message_text)

                _enqueue(sender_phone, bot_phone_id, message_id, message_text)

    return {"status": "ok"}


def _log_task_result(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    if (exc := task.exception()) is not None:
        log.error("Background task crashed: %r", exc, exc_info=exc)


async def _ack_nontext(sender_phone: str, bot_phone_id: str, mtype: str) -> None:
    """Acknowledge an image / voice note / document so the lead never gets
    silence. Screenshots at the payment stage get a real receipt message;
    everything else is gently steered back to text."""
    try:
        with _db() as conn:
            row = conn.execute("SELECT chat_stage FROM users WHERE user_phone = ?",
                               (sender_phone,)).fetchone()
        stage = row[0] if row else 0
        await asyncio.sleep(SHORT_GAP)
        if mtype == "image" and stage >= 10:
            text = MSG_SCREENSHOT                        # payment proof receipt
        elif mtype in ("audio", "voice", "ptt"):
            text = MSG_AUDIO                             # "I can't listen to audio, text please"
        else:
            text = MSG_TYPE_ONLY                         # anything else -> steer to text
        await asyncio.to_thread(meta.send_whatsapp_text, bot_phone_id, sender_phone, text)
        add_history(sender_phone, "bot", text)
        log.info("[WA] -> %s: (non-text ack) %.60s", sender_phone, text)
    except Exception:
        log.exception("Non-text ack failed for %s", sender_phone)


# ------------------------------------------------------------------ AI Generators
_FEMALE = ("You are Ani, a FEMALE health coach from 'livelyher'. Always speak as a "
           "woman: use feminine wording only, never masculine forms. Always address "
           "the customer directly as 'you' or 'your', never refer to her in third "
           "person as 'her' or 'she'.")

_ENGLISH = "Write strictly in simple, conversational English."

_NO_CLICHES = ("NEVER use empathy cliches like 'I understand your concern', 'I hear "
               "you', 'I totally understand', or similar filler. NEVER end with 'let "
               "me know if you have any other questions or concerns' or similar. Be "
               "direct, specific, and to the point.")


def evaluate_opening(user_msg: str, user_phone: str) -> dict:
    """Analyze the very first / opening messages so the bot actually LISTENS
    before it talks: answers any question she asked (from the FAQ knowledge
    base), spots refusal and pauses — instead of blindly dumping templates."""
    history = _history_block(user_phone)
    prompt = f"""{_FEMALE}
TASK: OPENING MESSAGE ANALYSIS
This is the very start of a WhatsApp conversation with a potential customer.
Recent conversation (if any):
{history}

FAQ KNOWLEDGE BASE FOR ANSWERING QUESTIONS (answer ONLY from this, never invent facts):
{FAQ_KNOWLEDGE_BASE}

Her opening message: "{user_msg}"

Decide:
1. answer: If any part of her message is a QUESTION or a request for information (about livelyher, location, price, the plan, delivery, who we are), write a short, direct, smart 1 or 2 sentence answer based ONLY on the FAQ KNOWLEDGE BASE, speaking to her as "you". If she asked no question, answer is null.
2. refusal: true only if she clearly declines or wants no contact ("no", "don't message me", "not interested", "stop"). Dodging, teasing, protests like "I didn't say anything", or rude but engaged replies are NOT refusal. Otherwise false.
3. pause: true only if she asks you to wait or says she is busy ("wait", "one minute", "busy"). Otherwise false.

CONSTRAINTS:
- {_ENGLISH}
- NEVER USE MARKDOWN and never use any hyphen or dash character. Plain text only.
- {_NO_CLICHES}

Return ONLY pure JSON in this format:
{{"answer": "string or null", "refusal": true/false, "pause": true/false}}"""

    response = groq().chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
    )
    result = json.loads(response.choices[0].message.content)
    if result.get("answer"):
        result["answer"] = _clean(result["answer"])
    return result


def _safe_opening(user_msg: str, user_phone: str) -> dict:
    try:
        return evaluate_opening(user_msg, user_phone)
    except Exception:
        log.exception("Opening analysis failed — continuing with plain greeting")
        return {}


def compose_stage1_bridge(user_msg: str, user_phone: str) -> dict:
    """The AI drives the transition into intake: it READS her reply, reacts to
    it naturally, answers any question from the FAQ knowledge base, and then
    invites her into the questions — replacing the blind MSG_2 template dump."""
    history = _history_block(user_phone)
    prompt = f"""{_FEMALE}
TASK: STAGE 1 BRIDGE
Context: you just greeted a new lead on WhatsApp (she received your Salam and intro).
Recent conversation:
{history}

FAQ KNOWLEDGE BASE FOR ANSWERING QUESTIONS (answer ONLY from this, never invent facts):
{FAQ_KNOWLEDGE_BASE}

She replied: "{user_msg}"

Compose your next message to her. 2 to 4 short sentences, under 55 words, in this natural order:
1. REACT to what she actually said (if she says she is fine, be glad; if she asked a question or asked where you are located, ANSWER it directly in 1 or 2 sentences using only the FAQ KNOWLEDGE BASE).
2. INVITE her warmly into a few quick questions and tell her what happens next: coaches will do a FREE detailed analysis of her situation and reach out in about 30 minutes; only once she is satisfied can she buy a personalized plan, delivered within 24 hours.
3. END with one short polite request to please reply in text messages, not voice messages.

Also classify:
- refusal: true ONLY if she clearly declines or wants no contact. Otherwise false.
- pause: true ONLY if she asked you to wait or said she is busy. Otherwise false.

CONSTRAINTS:
- {_ENGLISH}
- NEVER USE MARKDOWN and never use any hyphen or dash character. Plain text only.
- {_NO_CLICHES}
- Speak to her as "you"/"Ma'am", warm and human, no hype, and never open with a bare "Great,".

Return ONLY pure JSON in this format:
{{"text": "your bridge message", "refusal": true/false, "pause": true/false}}"""

    response = groq().chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
    )
    result = json.loads(response.choices[0].message.content)
    if result.get("text"):
        result["text"] = _clean(result["text"])
    return result


def _safe_bridge1(user_msg: str, user_phone: str) -> dict:
    try:
        return compose_stage1_bridge(user_msg, user_phone)
    except Exception:
        log.exception("Stage-1 bridge failed — falling back to MSG_2 template")
        return {"text": MSG_2}


def validate_answer(current_questions: str, user_message: str, user_phone: str) -> dict:
    history = _history_block(user_phone)
    prompt = f"""{_FEMALE}
The user was asked these questions: "{current_questions}"
Recent conversation:
{history}

FAQ KNOWLEDGE BASE FOR ANSWERING QUESTIONS:
{FAQ_KNOWLEDGE_BASE}

Their latest reply was: "{user_message}"

Task:
1. Did the user actually attempt to answer the questions (one or several messages combined count as one reply)? (It doesn't have to be perfect, just relevant to weight, diet, or stress depending on the question). If YES: is_valid = true.
2. If NO and the message is a PAUSE or DELAY message ("wait", "one minute", "brb", "I will be back", "busy right now", "ruko", "baad mein batati hoon"): is_valid = false, and reply_if_invalid is ONLY a short warm acknowledgment such as "Sure Ma'am, take your time. I am right here whenever you are ready." Do NOT repeat the questions, do NOT scold, and do NOT say you can only help with inquiries.
3. If NO and they are asking a valid question about livelyher: answer it directly in 1 or 2 short specific sentences based on the FAQ KNOWLEDGE BASE. Nothing else. Then politely ask them to answer the original questions.
4. If NO and the message is a REFUSAL or OPT-OUT (she clearly declines the program or wants no more contact, for example "no I don't wanna proceed", "not interested", "I don't want to order", "please stop"): is_valid = false, refusal = true, reply_if_invalid = null. The system sends a fixed graceful goodbye, so write nothing. IMPORTANT: refusal = true ONLY for a clear decline or stop request. Dodging the questions, teasing, impossible or made-up answers, "don't ask me", off-topic grumbling, or protests like "I didn't say anything" are NOT refusals — handle those as invalid under the other rules instead.
5. SANITY CHECK for measurements: if their claimed numbers are clearly impossible or contradict the program (weight far outside roughly 25 to 400 kg, height far outside roughly 3 to 8 feet or 90 to 245 cm, or a TARGET weight that is HIGHER than the current weight — we are a weight LOSS program): is_valid = false, refusal = false, and reply_if_invalid is ONE short smart sentence pointing out the implausible number and asking for the real values so we can help. Do NOT repeat the whole question set and do NOT scold.
6. If NO and totally off-topic: politely say you can only assist with livelyher inquiries, and repeat the questions.

CONSTRAINTS:
- {_ENGLISH}
- NEVER USE MARKDOWN (no *, #, -, etc) and never use any hyphen or dash character. Plain text only.
- {_NO_CLICHES}

Return ONLY pure JSON in this format:
{{
  "is_valid": true or false,
  "refusal": true or false,
  "reply_if_invalid": "Your response here if false (and not a refusal), else null"
}}"""

    response = groq().chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
    )
    result = json.loads(response.choices[0].message.content)
    if result.get("reply_if_invalid"):
        result["reply_if_invalid"] = _clean(result["reply_if_invalid"])
    return result


def evaluate_intent(user_msg: str, user_phone: str) -> dict:
    history = _history_block(user_phone)
    prompt = f"""{_FEMALE}
The user is in a consultation funnel. Here is the recent conversation for context:
{history}

FAQ KNOWLEDGE BASE FOR ANSWERING QUESTIONS:
{FAQ_KNOWLEDGE_BASE}

User just said: "{user_msg}"

Classify the user's LATEST message, following these rules strictly:

RULE A - REPLY LAG: People read and reply late. Her message may be answering an EARLIER bot message, not the most recent one. Look at the conversation history and decide WHICH bot message she is actually responding to. If she is clearly reacting to something older (for example talking about the video, the analysis, or an earlier question) while a NEWER question is still open and she did not answer that newer question, that is NOT fresh agreement: is_valid = false.
RULE B - MIXED SIGNALS: If a quick "ok / yes / theek hai" is bundled with ANY hesitation, delay, condition, inability, or question ("ok but...", "ok I will watch the video later and then decide", "wait one minute", "I can't right now"), HESITATION WINS: is_valid = false.

RULE C - REFUSAL / OPT-OUT: If she clearly declines or wants out ("no", "not interested", "I don't want it", "I don't want to order", "I don't want to continue", "stop messaging me", "leave me alone", "don't contact me again", or any firm angry refusal), set stop = true and is_valid = false. The reply is then ONE short graceful goodbye sentence with zero pressure and zero questions (for example "No problem at all Ma'am, thank you for your time."). Do NOT re-ask anything and do NOT invite her to continue. Teasing, dodging the questions, fake or impossible answers, protests like "I didn't say anything", or rude but still engaged replies are NEVER stop — handle them under rule 2 instead.

1. is_valid TRUE only for a CLEAR, UNAMBIGUOUS agreement, confirmation, or presence aimed at the bot's CURRENT open question (e.g. "I am here", "yes", "ok", "sure", "send it", "I watched it", "I am ready", "payment done").
2. is_valid FALSE for HESITATION or DELAY ("let me think", "I need time", "not right now", "I can't purchase now", "later", "I will watch it later"), OBJECTIONS (price, trust, doubts), QUESTIONS, COMPLAINTS, or any lagging reply covered by RULE A or RULE B. Then write the reply like this:
   - For a QUESTION or OBJECTION: answer it directly in 1 or 2 short, specific, smart sentences. Nothing else. Do NOT ask if she has more questions or concerns, and do NOT push her.
   - For HESITATION or DELAY: write one short warm sentence with zero pressure, and steer the conversation back into the process. CHECK THE HISTORY first:
     * If this is her FIRST time hesitating at this step, simply comfort her, in fresh wording.
     * If you already comforted her about waiting recently, do NOT just say "take your time" again. Comfort briefly AND gently invite her back into the CURRENT step you were on, referencing the last open point (for example: "your detailed analysis is already done Ma'am, shall I continue from there?", or "shall I send you the questions so we can start your plan today?", or "the 51 percent discount closes today Ma'am, want me to hold your spot?").
     Every hesitation reply must quietly guide her back to the next step while staying warm and respectful.
   - Never use empty empathy phrases. Answer to the point. Never send the same sentence twice: read the history and keep your wording fresh every single time.

CONSTRAINTS:
- {_ENGLISH}
- NEVER USE MARKDOWN (no *, #, -, etc) and never use any hyphen or dash character. Plain text only.
- {_NO_CLICHES}
- When unsure between agreement and hesitation, choose hesitation.

Return ONLY pure JSON in this format:
{{"is_valid": true/false, "reply": "string or null", "stop": true/false}}"""

    response = groq().chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    result = json.loads(response.choices[0].message.content)
    if result.get("reply"):
        result["reply"] = _clean(result["reply"])
    return result


def _intent_or_hold(user_msg: str, user_phone: str) -> dict:
    """Never let an AI hiccup freeze the lead mid-pitch."""
    try:
        return evaluate_intent(user_msg, user_phone)
    except Exception:
        log.exception("Intent check crashed — holding the stage politely")
        return {"is_valid": False, "reply": None}


# Pure, unambiguous acknowledgments must never be re-judged by the LLM:
# "ok" means ok. (Anything longer — "ok but...", questions, delays — still
# goes through evaluate_intent for the nuance.)
_ACK_WORDS = {
    "ok", "okay", "oki", "okie", "okk", "okayyy", "okay mam", "okay ma'am",
    "ok mam", "ok ma'am", "ji", "jee", "yes", "yess", "yes please", "yes mam",
    "yes ma'am", "haan", "han", "sure", "theek", "theek hai", "theek hy",
    "acha", "achaa", "acha ji", "acha jee", "go ahead", "send it", "send",
    "i am here", "im here", "i'm here", "ready", "i am ready", "watched",
    "watched it", "seen", "seen it", "dekh li", "done", "yes done",
}


def _is_pure_ack(message_text: str) -> bool:
    t = re.sub(r"[^a-zA-Z' ]", " ", message_text.lower())
    t = re.sub(r"\s+", " ", t).strip()
    return t in _ACK_WORDS


def generate_medical_pitch(a1, a2, a3) -> str:
    prompt = f"""{_FEMALE}
The user's data: Physical: {a1} | Diet: {a2} | Stress: {a3}

Write exactly ONE paragraph in simple English using this exact structure (fill in the brackets based on their data):
"The situation you explained (mention specific situation causing effect) suggests you have (their medical conditions like insulin sensitivity or pcos or obesity whatever fits them). In this (simply scientifically explain what happens in it relating to them) because of which (their pain or problem they told they are suffering)."

GOAL: educate them about their problem. This is critical: use scientific terms but explain them simply enough that they understand.

CONSTRAINTS:
- {_ENGLISH}
- DO NOT add extra greetings or endings. Keep the answer to a single paragraph.
- STRICTLY NO MARKDOWN and no hyphen or dash characters anywhere. Plain text only."""
    response = groq().chat.completions.create(model=GROQ_MODEL, messages=[{"role": "user", "content": prompt}])
    return _clean(response.choices[0].message.content)


def _safe_medical_pitch(a1, a2, a3) -> str | None:
    """Try to generate the analysis; return None (not a crash) if the LLM is
    unavailable — the scheduler will retry at delivery time."""
    try:
        return generate_medical_pitch(a1, a2, a3)
    except Exception:
        log.exception("Could not pre-generate analysis — will retry on delivery")
        return None


def generate_fear_pitch(a1, a2, a3) -> str:
    prompt = f"""{_FEMALE}
User's data: Physical: {a1} | Diet: {a2} | Stress: {a3}

Write exactly ONE paragraph in simple English using this exact structure:
"In long term it can cause (mention ONLY 2 or 3 realistic things that could happen if this is left untreated, chosen to fit THEIR specific condition — for example reaching 90+ weight, diabetes, losing body shape, or similar)."

GOAL: educate them about what might happen if action is not taken now, strictly according to THEIR condition. Do NOT intimidate and do NOT make the fear paralyzing: only 2 or 3 possibilities, just enough so they know it can lead to a worse and harder to solve problem in future.

CONSTRAINTS:
- {_ENGLISH}
- DO NOT add extra greetings or endings. Keep the answer to a single paragraph.
- STRICTLY NO MARKDOWN and no hyphen or dash characters anywhere. Plain text only."""
    response = groq().chat.completions.create(model=GROQ_MODEL, messages=[{"role": "user", "content": prompt}])
    return _clean(response.choices[0].message.content)


def generate_plan_explain(a1, a2, a3) -> str:
    prompt = f"""{_FEMALE}
User's data: Physical: {a1} | Diet: {a2} | Stress: {a3}

In one or two very short simple English sentences, explain how a few diet changes plus vitamins and a tea will scientifically address their specific problem and cause weight loss within weeks. Start the sentence continuing naturally after the words "this will" (for example start with a verb like "fix", "reduce", "balance").

CONSTRAINTS:
- {_ENGLISH}
- No greetings, no extra sentences, no advice beyond that explanation.
- STRICTLY NO MARKDOWN and no hyphen or dash characters anywhere. Plain text only."""
    response = groq().chat.completions.create(model=GROQ_MODEL, messages=[{"role": "user", "content": prompt}])
    return _clean(response.choices[0].message.content)


def _validate_or_accept(questions: str, message_text: str, user_phone: str) -> dict:
    try:
        return validate_answer(questions, message_text, user_phone)
    except Exception:
        log.exception("Validation crashed — accepting answer to keep funnel moving")
        return {"is_valid": True}


# ------------------------------------------------------------------ Core Logic Flow
async def _dispatch_safe(sender_phone: str, bot_phone_id: str, message_text: str) -> None:
    try:
        await _dispatch(sender_phone, bot_phone_id, message_text)
    except _BurstAborted:
        log.info("--> [BARGE-IN] %s spoke mid-burst: remaining messages skipped, "
                 "stage NOT advanced — her message gets evaluated at the right step",
                 sender_phone)
    except Exception:
        log.exception("Dispatch failed for %s", sender_phone)


async def _dispatch(sender_phone: str, bot_phone_id: str, message_text: str) -> None:
    _interrupted.discard(sender_phone)
    with _db() as conn:
        cursor = conn.execute(
            "SELECT chat_stage, answers_1, answers_2, answers_3, analysis_text, resume_stage "
            "FROM users WHERE user_phone = ?", (sender_phone,))
        row = cursor.fetchone()

    async def send(text: str):
        # A bare video link (or WhatsApp media id) becomes a NATIVE video
        # message — inline player with its own real thumbnail and zero link
        # preview dependency. Everything else is text (links render a preview
        # card automatically now that preview_url is on).
        if _is_video_payload(text):
            await asyncio.to_thread(meta.send_whatsapp_video, bot_phone_id,
                                    sender_phone, text.strip())
        else:
            await asyncio.to_thread(meta.send_whatsapp_text, bot_phone_id, sender_phone, text)
        add_history(sender_phone, "bot", text)
        log.info("[WA] -> %s: %.80s", sender_phone, text)

    async def csend(text: str, dedupe: bool = True):
        """Burst-aware send with resume semantics:
        1. Aborts the remaining burst the moment she speaks (barge-in).
        2. NEVER repeats a message already delivered to her — if this exact
           text is already in her chat history (e.g. the burst was aborted
           and is being resumed), it is skipped rather than sent twice.
           Replies (AI-generated holds, payment confirmation) use
           dedupe=False so she is never left on silence."""
        if sender_phone in _interrupted:
            raise _BurstAborted
        if dedupe and _already_sent(sender_phone, text):
            log.info("[WA] -> %s: (skip, already sent) %.60s", sender_phone, text)
            return
        await send(text)

    # STATE 0: NEW USER (30s delay) — but first ANALYZE her opening message so
    # the bot LISTENS before it talks: instant opt-outs park immediately, and
    # any question she asked gets its answer right after the greeting.
    if not row:
        with _db() as conn:
            conn.execute("INSERT INTO users (user_phone, bot_phone_id, chat_stage) VALUES (?, ?, 1)",
                         (sender_phone, bot_phone_id))

        opening = await asyncio.to_thread(_safe_opening, message_text, sender_phone)
        if opening.get("refusal"):
            log.info("--> [OPT-OUT] %s refused on first contact", sender_phone)
            await asyncio.sleep(SHORT_GAP)
            await send(MSG_GOODBYE)
            with _db() as conn:
                conn.execute("UPDATE users SET resume_stage = 1, chat_stage = ? "
                             "WHERE user_phone = ?", (STAGE_OPTED_OUT, sender_phone))
            return

        log.info("--> [NEW USER] Delaying 30s before first response to %s", sender_phone)
        await asyncio.sleep(30)
        await send(MSG_1)
        if opening.get("answer"):
            await asyncio.sleep(INTAKE_GAP)
            await send(opening["answer"])
        return

    stage, a1, a2, a3, stored_analysis, resume_stage = row

    with _db() as conn:
        conn.execute("UPDATE users SET bot_phone_id = ? WHERE user_phone = ?",
                     (bot_phone_id, sender_phone))

    # OPT-OUT RESIDENCY: she said stop. Only a clear re-engagement brings her
    # back — resumed exactly where she left off, never re-asked, never restarted.
    if stage in (STAGE_OPTED_OUT, STAGE_OPTED_OUT_HARD):
        # A bare "ok/okay/ji" after the goodbye is just her acknowledging the
        # message — it is NOT consent to resume. Unparking on filler was the
        # welcome-back loop the user reported. Say nothing, stay parked.
        if _is_pure_ack(message_text):
            log.info("--> [PARKED-ACK] %s sent filler after goodbye — staying parked, silent",
                     sender_phone)
            return
        chk = await asyncio.to_thread(_intent_or_hold, message_text, sender_phone)
        if chk.get("stop"):
            if stage == STAGE_OPTED_OUT:
                await asyncio.sleep(SHORT_GAP)
                await csend(MSG_OPTOUT_FINAL, dedupe=False)
                with _db() as conn:
                    conn.execute("UPDATE users SET chat_stage = ? WHERE user_phone = ?",
                                 (STAGE_OPTED_OUT_HARD, sender_phone))
            else:
                log.info("--> [OPT-OUT] %s still declining — respectfully silent",
                         sender_phone)
            return
        await asyncio.sleep(SHORT_GAP)
        await csend(MSG_WELCOME_BACK, dedupe=False)
        resume = resume_stage if 0 < resume_stage < STAGE_OPTED_OUT else 1
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = ? WHERE user_phone = ?",
                         (resume, sender_phone))
        log.info("--> [RESUMED] %s came back — continuing at stage %s",
                 sender_phone, resume)
        stage = resume

    # Smart Intent Checker (stage 6+): hesitation/objections/questions no
    # longer advance the funnel — only clear agreement does. A pure "ok" IS
    # clear agreement: skip the LLM for it entirely (it used to get
    # misjudged as hesitation, producing endless "take your time" loops).
    if stage >= 6:
        if _is_pure_ack(message_text):
            intent = {"is_valid": True, "reply": None, "stop": False}
            log.info("--> [ACK] %s sent a pure acknowledgment — advancing, no AI needed",
                     sender_phone)
        else:
            intent = await asyncio.to_thread(_intent_or_hold, message_text, sender_phone)
        if intent.get("stop"):
            log.info("--> [OPT-OUT] %s declined at stage %s — graceful goodbye, parked",
                     sender_phone, stage)
            await asyncio.sleep(SHORT_GAP)
            await csend(intent.get("reply") or MSG_GOODBYE, dedupe=False)
            with _db() as conn:
                conn.execute("UPDATE users SET resume_stage = ?, chat_stage = ? "
                             "WHERE user_phone = ?", (stage, STAGE_OPTED_OUT, sender_phone))
            return
        if not intent.get("is_valid"):
            log.info("--> [HOLD] Objection/question/hesitation — stage %s frozen.", stage)
            await asyncio.sleep(SHORT_GAP)
            hold = intent.get("reply") or HESITATION_FALLBACK
            if _last_bot_message(sender_phone) == hold:
                # Never send her the identical comfort line twice in a row —
                # being read with silence beats looking like a broken loop.
                log.info("--> [ANTI-REPEAT] identical hold suppressed for %s — waiting silently",
                         sender_phone)
            else:
                await csend(hold, dedupe=False)
            return

    # STATE MACHINE ADVANCEMENT (TIERED PACING: 4s intake, 7s short, 12s big)
    if stage == 1:
        # The AI drives this transition: it reads her reply, reacts to it,
        # answers her question, and only THEN invites her into the questions.
        # A pure ack right after the greeting ("ok", "ok ji") is simply a green
        # light — go straight to the intake invite, never judge it as pause.
        if _is_pure_ack(message_text):
            log.info("--> [ACK] %s green-lit intake from the greeting", sender_phone)
            op = {"text": None, "refusal": False, "pause": False}
        else:
            op = await asyncio.to_thread(_safe_bridge1, message_text, sender_phone)
        if op.get("refusal"):
            log.info("--> [OPT-OUT] %s declined at stage 1", sender_phone)
            await asyncio.sleep(SHORT_GAP)
            await send(MSG_GOODBYE)
            with _db() as conn:
                conn.execute("UPDATE users SET resume_stage = 1, chat_stage = ? "
                             "WHERE user_phone = ?", (STAGE_OPTED_OUT, sender_phone))
            return
        if op.get("pause"):
            await asyncio.sleep(INTAKE_GAP)
            if _last_bot_message(sender_phone) == MSG_PAUSE:
                log.info("--> [ANTI-REPEAT] pause msg already sent to %s — waiting silently",
                         sender_phone)
            else:
                await send(MSG_PAUSE)
            return
        await asyncio.sleep(INTAKE_GAP)
        await send(op.get("text") or MSG_2)
        await asyncio.sleep(SHORT_GAP)   # a human beat before the question set
        await send(SET_1)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 2 WHERE user_phone = ?", (sender_phone,))

    elif stage == 2:
        if _is_pure_ack(message_text):
            # Filler ("ok", "ok??") with no answers — say nothing, just wait
            # for her real reply. Never re-send the same nudge in a loop.
            log.info("--> [ACK-HOLD] %s sent a filler ack mid-intake — waiting silently", sender_phone)
            return
        val = await asyncio.to_thread(_validate_or_accept, SET_1, message_text, sender_phone)
        await asyncio.sleep(INTAKE_GAP)
        if val.get("refusal"):
            log.info("--> [OPT-OUT] %s declined during intake (stage 2) — parked", sender_phone)
            await send(MSG_GOODBYE)
            with _db() as conn:
                conn.execute("UPDATE users SET resume_stage = 2, chat_stage = ? "
                             "WHERE user_phone = ?", (STAGE_OPTED_OUT, sender_phone))
        elif val.get("is_valid"):
            with _db() as conn:
                conn.execute("UPDATE users SET chat_stage = 3, answers_1 = ? WHERE user_phone = ?",
                             (message_text, sender_phone))
            await send(SET_2)
        else:
            reply = val.get("reply_if_invalid") or INVALID_FALLBACK
            if _last_bot_message(sender_phone) == reply:
                log.info("--> [ANTI-REPEAT] identical intake nudge suppressed for %s — silent",
                         sender_phone)
            else:
                await send(reply)

    elif stage == 3:
        if _is_pure_ack(message_text):
            log.info("--> [ACK-HOLD] %s sent a filler ack mid-intake — waiting silently", sender_phone)
            return
        val = await asyncio.to_thread(_validate_or_accept, SET_2, message_text, sender_phone)
        await asyncio.sleep(INTAKE_GAP)
        if val.get("refusal"):
            log.info("--> [OPT-OUT] %s declined during intake (stage 3) — parked", sender_phone)
            await send(MSG_GOODBYE)
            with _db() as conn:
                conn.execute("UPDATE users SET resume_stage = 3, chat_stage = ? "
                             "WHERE user_phone = ?", (STAGE_OPTED_OUT, sender_phone))
        elif val.get("is_valid"):
            with _db() as conn:
                conn.execute("UPDATE users SET chat_stage = 4, answers_2 = ? WHERE user_phone = ?",
                             (message_text, sender_phone))
            await send(SET_3)
        else:
            reply = val.get("reply_if_invalid") or INVALID_FALLBACK
            if _last_bot_message(sender_phone) == reply:
                log.info("--> [ANTI-REPEAT] identical intake nudge suppressed for %s — silent",
                         sender_phone)
            else:
                await send(reply)

    elif stage == 4:
        if _is_pure_ack(message_text):
            log.info("--> [ACK-HOLD] %s sent a filler ack mid-intake — waiting silently", sender_phone)
            return
        val = await asyncio.to_thread(_validate_or_accept, SET_3, message_text, sender_phone)
        await asyncio.sleep(INTAKE_GAP)
        if val.get("refusal"):
            log.info("--> [OPT-OUT] %s declined during intake (stage 4) — parked", sender_phone)
            await send(MSG_GOODBYE)
            with _db() as conn:
                conn.execute("UPDATE users SET resume_stage = 4, chat_stage = ? "
                             "WHERE user_phone = ?", (STAGE_OPTED_OUT, sender_phone))
        elif val.get("is_valid"):
            await send(MSG_END)
            await asyncio.sleep(INTAKE_GAP)
            await send(ABOUT_VIDEO)

            # Pre-generate the analysis so stage 6 can reuse it instantly.
            analysis = await asyncio.to_thread(_safe_medical_pitch, a1, a2, message_text)

            # --- TESTING BYPASS: skip the 30-minute timer entirely ---
            await asyncio.sleep(12)
            await send(PITCH_1)
            with _db() as conn:
                conn.execute(
                    "UPDATE users SET chat_stage = 6, answers_3 = ?, analysis_send_at = ?, "
                    "analysis_text = ? WHERE user_phone = ?",
                    (message_text, datetime.datetime.now(datetime.timezone.utc).isoformat(),
                     analysis, sender_phone))
            log.info("--> [TESTING] 30-min timer bypassed for %s — straight to stage 6",
                     sender_phone)
        else:
            reply = val.get("reply_if_invalid") or INVALID_FALLBACK
            if _last_bot_message(sender_phone) == reply:
                log.info("--> [ANTI-REPEAT] identical intake nudge suppressed for %s — silent",
                         sender_phone)
            else:
                await send(reply)

    elif stage == 5:
        # Still inside the 30-minute wait — hold them gently, but never as a
        # broken record: filler acks get silence, and the wait note sends once.
        if _is_pure_ack(message_text):
            log.info("--> [ACK-HOLD] %s sent a filler ack during the 30-min wait — silent", sender_phone)
            return
        await asyncio.sleep(SHORT_GAP)
        if _last_bot_message(sender_phone) == MSG_WAIT:
            log.info("--> [ANTI-REPEAT] wait note already sent to %s — silent", sender_phone)
        else:
            await send(MSG_WAIT)

    elif stage == 6:
        # Lead replied to PITCH_1 ("I am here") -> STORED Message 2 + Message 3
        msg2 = stored_analysis
        if not msg2:
            msg2 = await asyncio.to_thread(generate_medical_pitch, a1, a2, a3)
            with _db() as conn:
                conn.execute("UPDATE users SET analysis_text = ? WHERE user_phone = ?",
                             (msg2, sender_phone))
        await _auto_gap(msg2)
        await csend(msg2)
        await _auto_gap(PITCH_3)
        await csend(PITCH_3)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 7 WHERE user_phone = ?", (sender_phone,))

    elif stage == 7:
        # Generated once per lead, cached — so a barge-in RESUME never
        # regenerates a slightly different duplicate of the same pitch.
        msg4 = _gen_cache.get((sender_phone, "fear"))
        if not msg4:
            msg4 = await asyncio.to_thread(generate_fear_pitch, a1, a2, a3)
            _gen_cache[(sender_phone, "fear")] = msg4
        clause6 = _gen_cache.get((sender_phone, "clause6"))
        if not clause6:
            clause6 = await asyncio.to_thread(generate_plan_explain, a1, a2, a3)
            _gen_cache[(sender_phone, "clause6")] = clause6
        fill6 = PITCH_6_TEMPLATE.replace("{AI_EXPLAIN}", clause6)
        await _auto_gap(msg4)
        await csend(msg4)
        await _auto_gap(PITCH_5)
        await csend(PITCH_5)
        await _auto_gap(fill6)
        await csend(fill6)
        await _auto_gap(PITCH_7)
        await csend(PITCH_7)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 8 WHERE user_phone = ?", (sender_phone,))

    elif stage == 8:
        await _auto_gap(PITCH_8)
        await csend(PITCH_8)
        await asyncio.sleep(VIDEO_PRE_GAP)    # doc: video appears after 5 seconds
        await csend(PITCH_9)
        await asyncio.sleep(VIDEO_POST_GAP)   # doc: Message 11 appears 3 seconds after the video
        await csend(PITCH_10)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 9 WHERE user_phone = ?", (sender_phone,))

    elif stage == 9:
        await _auto_gap(PITCH_11)
        await csend(PITCH_11)
        await asyncio.sleep(PAIR_GAP)   # 3s: price and scarcity land as one thought
        await csend(PITCH_13)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 10 WHERE user_phone = ?", (sender_phone,))

    elif stage == 10:
        await _auto_gap(PITCH_15)
        await csend(PITCH_15)
        await _auto_gap(PITCH_16)
        await csend(PITCH_16)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 11 WHERE user_phone = ?", (sender_phone,))

    elif stage >= 11:
        # Payment stage: never go silent again. They agreed/confirmed —
        # ask for the payment screenshot to close the loop.
        await _auto_gap(MSG_CONFIRM_PAYMENT)
        await csend(MSG_CONFIRM_PAYMENT, dedupe=False)
