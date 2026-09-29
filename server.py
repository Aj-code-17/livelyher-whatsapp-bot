"""Livelyher — WhatsApp conversational CRM & intake bot.

Receives WhatsApp Cloud API webhooks from Meta, walks each lead through a
6-stage Roman-Urdu intake funnel (with LLM answer validation), waits 30
minutes, then sends a personalized AI weight-loss coaching pitch.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from groq import Groq  # note: package name is lowercase

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
# The model the client uses: Alibaba Qwen3.8-27B on Groq (preview tier).
GROQ_MODEL = os.getenv("GROQ_MODEL", "qwen/qwen3.8-27b")
ANALYSIS_DELAY_MINUTES = float(os.getenv("ANALYSIS_DELAY_MINUTES", "30"))

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


# ------------------------------------------------------------ conversation template
MSG_1 = "Asslamualikum! it's Ani from livelyher, how are you Ma'am?"
MSG_2 = "Great, I will ask you some basic questions, then we will analyse your situation and reach out to you in 30 minutes where we will explain your situation in detail and how we will help you fix it, Inshallah!"
SET_1 = "Kindly tell us about:\n1. Aapka Current Weight aur Target Weight (kg) kitna hai, aur aapki Height kya hai?\n2. Ye weight gain kab shuru hua, shaadi ke baad, pregnancy/delivery ke baad, ya pichle 1–2 saalon mein achanak barha?\n3. Body mein stubborn weight sabse zyada kahan mehsoos hota hai — lower belly/stomach fat, hips/thighs, ya overall heavy bloating?"
SET_2 = "4. Pehle weight loss ke liye kya try kiya hai, crash diet, green teas, meal skipping, ya intermittent fasting and usei faida hua?\n5. Aapki daily eating routine kaisi rehti hai, exactly what you usually eat in breakfast, lunch, dinner and snacking?\n6. Kya koi hormonal blocker ya issue hai jiski wajah se weight drop nahi hota (jaise PCOS, Thyroid, ya irregular cycles)?"
SET_3 = "For Understanding your Mood and Stress Level:\n1. 1 se 10 ke scale par aap apna daily anxiety aur mental stress kis number par rank karengi?\n2. Aapki sleep routine kaisi rehti hai, kya raat ko sote waqt mind switch off nahi hota ya neend toot-toot kar aati hai, aur subah uthne par energy bilkul low hoti hai?\n3. Aapko stress ya anxiety feel hoti hai? Ya aise lage kei jin cheezun ki pehlay enjoy krte that wo ab achi nai lagtin? Ya choti choti baat per gussa ya irritability hoti hoo?"
MSG_END = "Thanks for sharing information Mam, we will analyse your situation and reach out to you in about 30 minutes.\n\nWe will explain you in detail your issue, why it is happening and how we can help you, and only once you are satisfied you can buy your Personalized plan, which will be delivered to you! 😇"
MSG_WAIT = ("Perfect Ma'am! 😊 Our coaches are analysing your answers right now — "
            "we'll reach out to you shortly, Inshallah.")
MSG_DONE = ("JazakAllah Ma'am! 😊 Our coach has already shared your analysis — "
            "the livelyher team will reach out to you shortly.")
INVALID_FALLBACK = ("Ma'am, could you please answer the questions above? 😊 "
                    "They help our coaches understand your situation properly.")


# ------------------------------------------------------------ database
def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")  # safe for scheduler + webhook threads
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
            analysis_send_at TEXT
        );
        CREATE TABLE IF NOT EXISTS seen_messages (message_id TEXT PRIMARY KEY);
    """)
    conn.commit()
    conn.close()


# ------------------------------------------------------- 30-minute scheduler
def check_scheduled_analyses() -> None:
    conn = None
    try:
        conn = _db()
        cursor = conn.cursor()
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        cursor.execute(
            "SELECT user_phone, bot_phone_id, answers_1, answers_2, answers_3 "
            "FROM users WHERE chat_stage = 5 AND analysis_send_at <= ?", (now,))
        due = cursor.fetchall()

        for phone, bot_id, a1, a2, a3 in due:
            log.info("Generating 30-min AI analysis for %s (model=%s)", phone, GROQ_MODEL)

            prompt = f"""Act as an expert women's health and weight loss coach for 'livelyher'.
The client provided these answers regarding their health, diet, and stress:
Physical: {a1}
Dietary: {a2}
Mental/Stress: {a3}

Write a highly empathetic, detailed analysis strictly in Roman Urdu explaining why they are struggling to lose weight based on their answers, and pitch the livelyher Personalized Plan to fix it."""

            response = groq().chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
            )
            final_pitch = response.choices[0].message.content

            meta.send_whatsapp_text(bot_id or PHONE_NUMBER_ID, phone, final_pitch)
            log.info("[WA] -> %s: 30-min analysis sent (%.80s...)", phone, final_pitch)
            cursor.execute("UPDATE users SET chat_stage = 6 WHERE user_phone = ?", (phone,))
            conn.commit()  # commit per user so one failure can't stall the rest
    except Exception:
        log.exception("check_scheduled_analyses failed")
    finally:
        if conn:
            conn.close()


# Idempotent startup: FastAPI lifespan under uvicorn, plain import under WSGI.
_scheduler: BackgroundScheduler | None = None


def _start_background() -> None:
    global _scheduler
    setup_database()
    if _scheduler is None:
        _scheduler = BackgroundScheduler()
        _scheduler.add_job(check_scheduled_analyses, "interval", seconds=60)
        _scheduler.start()
        log.info("Livelyher started (model=%s, delay=%s min) — webhook ready",
                 GROQ_MODEL, ANALYSIS_DELAY_MINUTES)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _start_background()
    yield
    if _scheduler:
        _scheduler.shutdown()


app = FastAPI(title="Livelyher WhatsApp Bot", lifespan=lifespan)

_start_background()  # fallback for WSGI hosts where lifespan never fires


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
                    log.info("Ignoring non-text message type=%s", msg.get("type"))
                    continue

                sender_phone = msg.get("from")
                message_id = msg.get("id", "")
                message_text = (msg.get("text") or {}).get("body", "").strip()
                log.info("[WA] %s: %s", sender_phone, message_text)

                task = asyncio.create_task(asyncio.to_thread(
                    _dispatch_safe, sender_phone, bot_phone_id, message_id, message_text))
                task.add_done_callback(_log_task_result)

    # Always 200 fast — Meta retries deliveries on timeouts.
    return {"status": "ok"}


def _log_task_result(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    if (exc := task.exception()) is not None:
        log.error("Background dispatch crashed: %r", exc, exc_info=exc)


# ------------------------------------------------------------------ logic
def validate_answer(current_questions: str, user_message: str) -> dict:
    prompt = f"""You are Ani from 'livelyher', a women's health and weight-loss coaching service.
The user was asked these questions: "{current_questions}"
Their reply was: "{user_message}"

Task:
1. Did the user actually attempt to answer the questions? (It doesn't have to be perfect, just relevant to weight, diet, or stress depending on the question).
2. If NO: Are they asking a valid question about livelyher? If so, answer it briefly in Roman Urdu, then politely ask them to answer the original questions.
3. If NO and totally off-topic: Politely say you can only assist with livelyher inquiries, and repeat the questions.

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
    return json.loads(response.choices[0].message.content)


def _validate_or_accept(questions: str, message_text: str) -> dict:
    """If the validator itself errors (API hiccup, bad JSON), accept the
    answer instead of silently freezing a lead mid-funnel."""
    try:
        return validate_answer(questions, message_text)
    except Exception:
        log.exception("Validation crashed — accepting answer to keep funnel moving")
        return {"is_valid": True}


def _dispatch_safe(sender_phone: str, bot_phone_id: str,
                   message_id: str, message_text: str) -> None:
    try:
        _dispatch(sender_phone, bot_phone_id, message_id, message_text)
    except Exception:
        log.exception("Dispatch failed for %s", sender_phone)


def _dispatch(sender_phone: str, bot_phone_id: str,
              message_id: str, message_text: str) -> None:
    conn = _db()
    cursor = conn.cursor()

    # Dedupe Meta webhook retries so a user never double-advances
    if message_id:
        cursor.execute("INSERT OR IGNORE INTO seen_messages(message_id) VALUES (?)",
                       (message_id,))
        if cursor.rowcount == 0:
            log.info("Duplicate delivery of %s — skipping", message_id)
            conn.commit()
            conn.close()
            return

    def send(text: str) -> None:
        meta.send_whatsapp_text(bot_phone_id, sender_phone, text)
        log.info("[WA] -> %s: %.80s", sender_phone, text)

    try:
        cursor.execute("SELECT chat_stage FROM users WHERE user_phone = ?", (sender_phone,))
        row = cursor.fetchone()

        if not row:
            cursor.execute(
                "INSERT INTO users (user_phone, bot_phone_id, chat_stage) VALUES (?, ?, 1)",
                (sender_phone, bot_phone_id))
            conn.commit()
            send(MSG_1)
            return

        stage = row[0]
        cursor.execute("UPDATE users SET bot_phone_id = ? WHERE user_phone = ?",
                       (bot_phone_id, sender_phone))

        if stage == 1:
            send(MSG_2)
            send(SET_1)
            cursor.execute("UPDATE users SET chat_stage = 2 WHERE user_phone = ?",
                           (sender_phone,))

        elif stage == 2:
            val = _validate_or_accept(SET_1, message_text)
            if val.get("is_valid"):
                cursor.execute("UPDATE users SET chat_stage = 3, answers_1 = ? "
                               "WHERE user_phone = ?", (message_text, sender_phone))
                send(SET_2)
            else:
                send(val.get("reply_if_invalid") or INVALID_FALLBACK)

        elif stage == 3:
            val = _validate_or_accept(SET_2, message_text)
            if val.get("is_valid"):
                cursor.execute("UPDATE users SET chat_stage = 4, answers_2 = ? "
                               "WHERE user_phone = ?", (message_text, sender_phone))
                send(SET_3)
            else:
                send(val.get("reply_if_invalid") or INVALID_FALLBACK)

        elif stage == 4:
            val = _validate_or_accept(SET_3, message_text)
            if val.get("is_valid"):
                send_time = (datetime.datetime.now(datetime.timezone.utc)
                             + datetime.timedelta(minutes=ANALYSIS_DELAY_MINUTES)).isoformat()
                cursor.execute("UPDATE users SET chat_stage = 5, answers_3 = ?, "
                               "analysis_send_at = ? WHERE user_phone = ?",
                               (message_text, send_time, sender_phone))
                send(MSG_END)
            else:
                send(val.get("reply_if_invalid") or INVALID_FALLBACK)

        elif stage >= 5:
            # While waiting for analysis, or after the pitch was delivered.
            send(MSG_WAIT if stage == 5 else MSG_DONE)

        conn.commit()
    finally:
        conn.close()
