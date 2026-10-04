"""Livelyher — TESTING BUILD (do NOT deploy to production).

Identical to server.py EXCEPT: the 30-minute analysis timer is bypassed.
Intake end -> MSG_END -> about video -> ~12s -> PITCH_1 -> stage 6 instantly.
Everything else (v4) is unchanged: barge-in burst abort, reply-lag-aware
intent checker, pause handling, non-text/screenshot acknowledgments,
hesitation/payment fixes, pre-generated analysis reuse, debounce batching,
sanitizer, chat history, wake-up sweeps.

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
DEBOUNCE_MIN = float(os.getenv("DEBOUNCE_MIN_SECONDS", "15"))
DEBOUNCE_MAX = float(os.getenv("DEBOUNCE_MAX_SECONDS", "20"))

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
MSG_2 = "Great, I will ask you some basic questions, then we will analyse your situation and reach out to you in 30 minutes where we will explain your situation in detail and how we will help you fix it, Inshallah!"
SET_1 = "Kindly tell us about:\n1. Aapka Current Weight aur Target Weight (kg) kitna hai, aur aapki Height kya hai?\n2. Ye weight gain kab shuru hua, shaadi ke baad, pregnancy/delivery ke baad, ya pichle 1–2 saalon mein achanak barha?\n3. Body mein stubborn weight sabse zyada kahan mehsoos hota hai — lower belly/stomach fat, hips/thighs, ya overall heavy bloating?"
SET_2 = "4. Pehle weight loss ke liye kya try kiya hai, crash diet, green teas, meal skipping, ya intermittent fasting and usei faida kyun hua?\n5. Aapki daily eating routine kaisi rehti hai, exactly what you usually eat in breakfast, lunch, dinner and snacking? iska answer thora detail mei dijye ga also tell the timing when you eat\n6. Kya koi hormonal blocker ya issue hai jiski wajah se weight drop nahi hota (jaise PCOS, Thyroid, ya irregular cycles)?"
SET_3 = "For Understanding your Mood and Stress Level:\n1. 1 se 10 ke scale par aap apna daily anxiety aur mental stress kis number par rank karengi (jahan 1 ka matlab bilkul calm aur 10 ka matlab extreme overthinking ya bechaini ho)?\n2. Aapki sleep routine kaisi rehti hai, kya raat ko sote waqt mind switch off nahi hota ya neend toot-toot kar aati hai, aur subah uthne par energy bilkul low hoti hai?\n3. Aapko stress ya anxiety feel hoti hai? Ya aise lage kei jin cheezun ki pehlay enjoy krte that wo ab achi nai lagtin? Ya choti choti baat per gussa ya irritability hoti hoo?"
MSG_END = "Thanks for sharing information Mam, we will analyse your situation and reach out to you in about 30 minutes\n\nWe will explain you in detail for FREE your issue, why it is happening and how we can help you and only once you are satisfied you can buy your Personalized plan, which will be delivered to you in 24hrs! 😇"
ABOUT_VIDEO = "In the meantime, please watch this video to know more about us: https://your-about-video-link-here.com"  # <--- ADD YOUR ABOUT VIDEO LINK HERE
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

# ---------------- DOCX PITCH TEMPLATE (Messages 1..17 as provided)
PITCH_1 = "Asslamualikum... we are done with the analysis, let me know when you are there Ma'am?"          # Message 1
PITCH_3 = "are you getting my point?"                                                                     # Message 3
PITCH_5 = "So we are setting a goal for you...we have to lose 6 to 7 kg weight in coming 6 weeks aur specially stress aur anxiety bilkul khatam krna hai because uskei bagair weight loss mushkil hota aur specially for women mood fresh aur lively hona bohat zaroori hota hai...."  # Message 5
PITCH_6_TEMPLATE = "So, for that, I will make a few changes in your diet and recommend few vitamins and a tea, this will {AI_EXPLAIN} and also follow the mood plan because it will help you a lot with mood and energy"  # Message 6
PITCH_7 = "And I am confident kei Insha'Allah in next 6 weeks we can achieve these results because first because we will design it exactly according to your routine you described so it will be very easy to follow and also, we will always be available to you whenever you need any help.."  # Message 7
PITCH_8 = "I am sharing the review video of one of our client so you better know how it is... they ordered a printed version"  # Message 8
PITCH_9 = "https://your-video-link-here.com/video.mp4"  # Message 10 = VIDEO  <--- ADD YOUR VIDEO LINK HERE
PITCH_10 = "let me know once you have seen it, I will share more details than .."                          # Message 11
PITCH_11 = "The original price is 3000 it's on 51% discount for this so it will be 1470 only...aur for 4 weeks I will be there to support for any changes insha'Allah ☺️"  # Message 12
PITCH_13 = "Also Mam there are only 7 spots left in this batch aur aaj close hojaye ga….hum nei bohat detailed aur time laga ker analysis already krlia hai....lekin abhi kuch questions aur puchne hain regarding your diet preferences for making final plan...should I send you the questions?"  # Message 15
PITCH_15 = "Okay I will send you the questions aapko within 24 hrs plan miljay ga insha'Allah mei questions bana ker kuch deir mei bhejti hun..."  # Message 16
PITCH_16 = "For payment you can use following accounts:\n\nBank: [BANK NAME]\nAccount: [ACCOUNT NUMBER]\nTitle: Livelyher"  # Message 17  <--- ADD BANK DETAILS HERE


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


# ------------------------------------------------- 15-20s debounce machinery
_buffers: dict[str, list[str]] = {}
_workers: dict[str, asyncio.Task] = {}
_interrupted: set[str] = set()  # leads who spoke while we were mid-reply


class _BurstAborted(Exception):
    """The lead sent a new message while a multi-message burst was in flight."""


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
    _buffers.setdefault(sender_phone, []).append(message_text)
    if message_text.strip():
        _interrupted.add(sender_phone)  # fresh input: abort any in-flight burst ASAP
    worker = _workers.get(sender_phone)
    if worker is None or worker.done():
        task = asyncio.create_task(_conversation_worker(sender_phone, bot_phone_id))
        task.add_done_callback(_log_task_result)
        _workers[sender_phone] = task


async def _conversation_worker(sender_phone: str, bot_phone_id: str) -> None:
    """One worker per lead: waits 15-20s of silence, then processes everything
    that arrived — combined — in a single pass, so no double replies."""
    try:
        while True:
            await asyncio.sleep(random.uniform(DEBOUNCE_MIN, DEBOUNCE_MAX))
            texts = _buffers.pop(sender_phone, [])
            if not texts:
                break
            combined = "\n".join(t for t in texts if t).strip()
            if not combined:
                break
            if len(texts) > 1:
                log.info("--> [BATCHED] %d messages from %s combined into one reply",
                         len(texts), sender_phone)
            add_history(sender_phone, "user", combined)
            await _dispatch_safe(sender_phone, bot_phone_id, combined)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Conversation worker crashed for %s", sender_phone)
    finally:
        _workers.pop(sender_phone, None)


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
        await asyncio.sleep(6)
        text = MSG_SCREENSHOT if (mtype == "image" and stage >= 10) else MSG_TYPE_ONLY
        await asyncio.to_thread(meta.send_whatsapp_text, bot_phone_id, sender_phone, text)
        add_history(sender_phone, "bot", text)
        log.info("[WA] -> %s: (non-text ack) %.60s", sender_phone, text)
    except Exception:
        log.exception("Non-text ack failed for %s", sender_phone)


# ------------------------------------------------------------------ AI Generators
_FEMALE = ("You are Ani, a FEMALE health coach from 'livelyher'. Always speak as a "
           "woman: use feminine wording only, never masculine forms.")

_ENGLISH = "Write strictly in simple, conversational English."


def validate_answer(current_questions: str, user_message: str, user_phone: str) -> dict:
    history = _history_block(user_phone)
    prompt = f"""{_FEMALE}
The user was asked these questions: "{current_questions}"
Recent conversation:
{history}
Their latest reply was: "{user_message}"

Task:
1. Did the user actually attempt to answer the questions (one or several messages combined count as one reply)? (It doesn't have to be perfect, just relevant to weight, diet, or stress depending on the question). If YES: is_valid = true.
2. If NO and the message is a PAUSE or DELAY message ("wait", "one minute", "brb", "I will be back", "busy right now", "ruko", "baad mein batati hoon"): is_valid = false, and reply_if_invalid is ONLY a short warm acknowledgment such as "Sure Ma'am, take your time. I am right here whenever you are ready." Do NOT repeat the questions, do NOT scold, and do NOT say you can only help with inquiries.
3. If NO and they are asking a valid question about livelyher: answer it briefly, then politely ask them to answer the original questions.
4. If NO and totally off-topic: politely say you can only assist with livelyher inquiries, and repeat the questions.

CONSTRAINTS:
- {_ENGLISH}
- NEVER USE MARKDOWN (no *, #, -, etc) and never use any hyphen or dash character. Plain text only.

Return ONLY pure JSON in this format:
{{
  "is_valid": true or false,
  "reply_if_invalid": "Your response here if false, else null"
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

User just said: "{user_msg}"

Classify the user's LATEST message, following these rules strictly:

RULE A - REPLY LAG: People read and reply late. Her message may be answering an EARLIER bot message, not the most recent one. Look at the conversation history and decide WHICH bot message she is actually responding to. If she is clearly reacting to something older (for example talking about the video, the analysis, or an earlier question) while a NEWER question is still open and she did not answer that newer question, that is NOT fresh agreement: is_valid = false.
RULE B - MIXED SIGNALS: If a quick "ok / yes / theek hai" is bundled with ANY hesitation, delay, condition, inability, or question ("ok but...", "ok I will watch the video later and then decide", "wait one minute", "I can't right now"), HESITATION WINS: is_valid = false.

1. is_valid TRUE only for a CLEAR, UNAMBIGUOUS agreement, confirmation, or presence aimed at the bot's CURRENT open question (e.g. "I am here", "yes", "ok", "sure", "send it", "I watched it", "I am ready", "payment done").
2. is_valid FALSE for HESITATION or DELAY ("let me think", "I need time", "not right now", "I can't purchase now", "later", "I will watch it later"), OBJECTIONS (price, trust, doubts), QUESTIONS, COMPLAINTS, or any lagging reply covered by RULE A or RULE B. In this case write a polite, short, empathetic reply that matches exactly what she said (for example, if she cannot watch the video now, invite her to watch it whenever suits her and you will continue then), with zero pressure, and softly invite her to continue whenever she is ready.

CONSTRAINTS:
- {_ENGLISH}
- NEVER USE MARKDOWN (no *, #, -, etc) and never use any hyphen or dash character. Plain text only.
- When unsure between agreement and hesitation, choose hesitation.

Return ONLY pure JSON in this format:
{{"is_valid": true/false, "reply": "string or null"}}"""

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

Write exactly ONE paragraph in simple English using this exact structure to install a realistic fear element:
"In long term it can cause (tell what happens if it is left untreated, like reaching 90+ or 100+ weight, diabetes, losing body shape, penguin walk, black neck, pcos, arthritis, or whatever fits THEIR specific problem)."

GOAL: install the fear element of what might happen if they don't take action now, according to their condition. Do not make the fear paralyzing: just enough so they know it can lead to a worse and harder to solve problem in future.

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
            "SELECT chat_stage, answers_1, answers_2, answers_3, analysis_text "
            "FROM users WHERE user_phone = ?", (sender_phone,))
        row = cursor.fetchone()

    async def send(text: str):
        await asyncio.to_thread(meta.send_whatsapp_text, bot_phone_id, sender_phone, text)
        add_history(sender_phone, "bot", text)
        log.info("[WA] -> %s: %.80s", sender_phone, text)

    async def csend(text: str):
        """Burst-aware send: stops the rest of the burst the moment she speaks.

        Used for every pitch-stage message so her reply can never arrive 'too
        late' after we already advanced past her. The aborted burst simply
        stops; her buffered message is then evaluated at the CURRENT stage
        (the context she was actually answering in).
        """
        if sender_phone in _interrupted:
            raise _BurstAborted
        await send(text)

    # STATE 0: NEW USER (30 SECOND DELAY)
    if not row:
        with _db() as conn:
            conn.execute("INSERT INTO users (user_phone, bot_phone_id, chat_stage) VALUES (?, ?, 1)",
                         (sender_phone, bot_phone_id))

        log.info("--> [NEW USER] Delaying 30s before first response to %s", sender_phone)
        await asyncio.sleep(30)
        await send(MSG_1)
        return

    stage, a1, a2, a3, stored_analysis = row

    with _db() as conn:
        conn.execute("UPDATE users SET bot_phone_id = ? WHERE user_phone = ?",
                     (bot_phone_id, sender_phone))

    # Smart Intent Checker (stage 6+): hesitation/objections/questions no
    # longer advance the funnel — only clear agreement does.
    if stage >= 6:
        intent = await asyncio.to_thread(_intent_or_hold, message_text, sender_phone)
        if not intent.get("is_valid"):
            log.info("--> [HOLD] Objection/question/hesitation — stage %s frozen.", stage)
            await asyncio.sleep(10)
            await csend(intent.get("reply") or HESITATION_FALLBACK)
            return

    # STATE MACHINE ADVANCEMENT (WITH 10-15s DELAYS)
    if stage == 1:
        await asyncio.sleep(10)
        await send(MSG_2)
        await asyncio.sleep(12)
        await send(SET_1)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 2 WHERE user_phone = ?", (sender_phone,))

    elif stage == 2:
        val = await asyncio.to_thread(_validate_or_accept, SET_1, message_text, sender_phone)
        await asyncio.sleep(10)
        if val.get("is_valid"):
            with _db() as conn:
                conn.execute("UPDATE users SET chat_stage = 3, answers_1 = ? WHERE user_phone = ?",
                             (message_text, sender_phone))
            await send(SET_2)
        else:
            await send(val.get("reply_if_invalid") or INVALID_FALLBACK)

    elif stage == 3:
        val = await asyncio.to_thread(_validate_or_accept, SET_2, message_text, sender_phone)
        await asyncio.sleep(10)
        if val.get("is_valid"):
            with _db() as conn:
                conn.execute("UPDATE users SET chat_stage = 4, answers_2 = ? WHERE user_phone = ?",
                             (message_text, sender_phone))
            await send(SET_3)
        else:
            await send(val.get("reply_if_invalid") or INVALID_FALLBACK)

    elif stage == 4:
        val = await asyncio.to_thread(_validate_or_accept, SET_3, message_text, sender_phone)
        await asyncio.sleep(10)
        if val.get("is_valid"):
            await send(MSG_END)
            await asyncio.sleep(10)
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
            await send(val.get("reply_if_invalid") or INVALID_FALLBACK)

    elif stage == 5:
        # Still inside the 30-minute wait — hold them gently.
        await asyncio.sleep(10)
        await send(MSG_WAIT)

    elif stage == 6:
        # Lead replied to PITCH_1 ("I am here") -> STORED Message 2 + Message 3
        msg2 = stored_analysis
        if not msg2:
            msg2 = await asyncio.to_thread(generate_medical_pitch, a1, a2, a3)
            with _db() as conn:
                conn.execute("UPDATE users SET analysis_text = ? WHERE user_phone = ?",
                             (msg2, sender_phone))
        await asyncio.sleep(10)
        await csend(msg2)
        await asyncio.sleep(12)
        await csend(PITCH_3)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 7 WHERE user_phone = ?", (sender_phone,))

    elif stage == 7:
        msg4 = await asyncio.to_thread(generate_fear_pitch, a1, a2, a3)
        clause6 = await asyncio.to_thread(generate_plan_explain, a1, a2, a3)
        await asyncio.sleep(10)
        await csend(msg4)
        await asyncio.sleep(10)
        await csend(PITCH_5)
        await asyncio.sleep(10)
        await csend(PITCH_6_TEMPLATE.replace("{AI_EXPLAIN}", clause6))
        await asyncio.sleep(10)
        await csend(PITCH_7)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 8 WHERE user_phone = ?", (sender_phone,))

    elif stage == 8:
        await asyncio.sleep(10)
        await csend(PITCH_8)
        await asyncio.sleep(10)
        await csend(PITCH_9)
        await asyncio.sleep(10)
        await csend(PITCH_10)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 9 WHERE user_phone = ?", (sender_phone,))

    elif stage == 9:
        await asyncio.sleep(10)
        await csend(PITCH_11)
        await asyncio.sleep(10)
        await csend(PITCH_13)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 10 WHERE user_phone = ?", (sender_phone,))

    elif stage == 10:
        await asyncio.sleep(10)
        await csend(PITCH_15)
        await asyncio.sleep(10)
        await csend(PITCH_16)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 11 WHERE user_phone = ?", (sender_phone,))

    elif stage >= 11:
        # Payment stage: never go silent again. They agreed/confirmed —
        # ask for the payment screenshot to close the loop.
        await asyncio.sleep(10)
        await csend(MSG_CONFIRM_PAYMENT)
