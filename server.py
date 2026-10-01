"""Livelyher — WhatsApp conversational CRM & intake bot.

Receives WhatsApp Cloud API webhooks from Meta, walks each lead through a
multi-stage intake funnel (with LLM answer validation), and executes an
instant 15-step personalized AI consultative sales pitch with 10-15s typing delays.
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


# ------------------------------------------------------------ conversation templates
MSG_1 = "Asslamualikum! it's Ani from livelyher, how are you Ma'am?"
MSG_2 = "Great, I will ask you some basic questions, then we will analyse your situation and reach out to you in 30 minutes where we will explain your situation in detail and how we will help you fix it, Inshallah!"
SET_1 = "Kindly tell us about:\n1. Aapka Current Weight aur Target Weight (kg) kitna hai, aur aapki Height kya hai?\n2. Ye weight gain kab shuru hua, shaadi ke baad, pregnancy/delivery ke baad, ya pichle 1–2 saalon mein achanak barha?\n3. Body mein stubborn weight sabse zyada kahan mehsoos hota hai — lower belly/stomach fat, hips/thighs, ya overall heavy bloating?"
SET_2 = "4. Pehle weight loss ke liye kya try kiya hai, crash diet, green teas, meal skipping, ya intermittent fasting and usei faida hua?\n5. Aapki daily eating routine kaisi rehti hai, exactly what you usually eat in breakfast, lunch, dinner and snacking?\n6. Kya koi hormonal blocker ya issue hai jiski wajah se weight drop nahi hota (jaise PCOS, Thyroid, ya irregular cycles)?"
SET_3 = "For Understanding your Mood and Stress Level:\n1. 1 se 10 ke scale par aap apna daily anxiety aur mental stress kis number par rank karengi?\n2. Aapki sleep routine kaisi rehti hai, kya raat ko sote waqt mind switch off nahi hota ya neend toot-toot kar aati hai, aur subah uthne par energy bilkul low hoti hai?\n3. Aapko stress ya anxiety feel hoti hai? Ya aise lage kei jin cheezun ki pehlay enjoy krte that wo ab achi nai lagtin? Ya choti choti baat per gussa ya irritability hoti hoo?"
MSG_END = "Thanks for sharing information Mam, we will analyse your situation and reach out to you in about 30 minutes.\n\nWe will explain you in detail your issue, why it is happening and how we can help you, and only once you are satisfied you can buy your Personalized plan, which will be delivered to you! 😇"
MSG_WAIT = ("Perfect Ma'am! 😊 Our coaches are analysing your answers right now — "
            "we'll reach out to you shortly, Inshallah.")
INVALID_FALLBACK = ("Ma'am, could you please answer the questions above? 😊 "
                    "They help our coaches understand your situation properly.")

# NEW PITCH TEMPLATES (PITCH_12 Removed)
PITCH_1 = "Asslamualikum... we are done with the analysis, let me know when you are there Ma'am?"
PITCH_3 = "are you getting my point?"
PITCH_5 = "Insha'Allah in 4 weeks you will share a visible difference in your condition because at livelyher we do a lot of hardwork to specifically design the plan as per your problem and routine takei aik to follow krna bht asaan ho aur real aur results milsakain"
PITCH_6 = "I have read your routine we will just few changes in your diet aur saath aik tea aur kuch supplements bhi prescribe karain gei and specially mood plan usko must follow kijye ga it help a lot in lowering stress levels insha'Allah"
PITCH_7 = "aap kuch light exercise agar suggest karain to karlain gi?"
PITCH_8 = "I am sharing the review video of one of our client so you better know how it is... they ordered a printed version..."
PITCH_9 = "https://your-video-link-here.com/video.mp4" # <--- ADD YOUR VIDEO LINK HERE
PITCH_10 = "let me know once you have seen it, I will share more details than .."
PITCH_11 = "The original price is 3000 it's on 51% discount for this so it will be 1470 only...aur for 4 weeks I will be there to support for any changes insha'Allah ☺️"
PITCH_13 = "okay, so I am finalising your spot because only 7 are left for this batch aur aaj yeh close hojaye ga...."
PITCH_14 = "mei nei bohat detailed aur time laga ker analysis already krlia hai ....lekin abhi kuch questions aur puchne hain regarding your diet preferences for making final plan... should I send you the questions?"
PITCH_15 = "Okay I will send you the questions aapko within 24 hrs plan miljay ga insha'Allah mei questions bana ker kuch deir mei bhejti hun..."
PITCH_16 = "For payment you can use following accounts:\n\nBank: [BANK NAME]\nAccount: [ACCOUNT NUMBER]\nTitle: Livelyher" # <--- ADD BANK DETAILS HERE


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
            analysis_send_at TEXT
        );
        CREATE TABLE IF NOT EXISTS seen_messages (message_id TEXT PRIMARY KEY);
    """)
    conn.commit()
    conn.close()

# Scheduler logic disabled for testing purposes (bypassed in Stage 4)
def check_scheduled_analyses() -> None:
    pass 

_scheduler: BackgroundScheduler | None = None

def _start_background() -> None:
    global _scheduler
    setup_database()
    if _scheduler is None:
        _scheduler = BackgroundScheduler()
        _scheduler.add_job(check_scheduled_analyses, "interval", seconds=60)
        _scheduler.start()

@asynccontextmanager
async def lifespan(app: FastAPI):
    _start_background()
    yield
    if _scheduler:
        _scheduler.shutdown()

app = FastAPI(title="Livelyher WhatsApp Bot", lifespan=lifespan)
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

@app.post("/webhook", response_model=None)
async def receive_webhook(request: Request):
    body = await request.body()
    
    if not meta.verify_signature(body, request.headers.get("X-Hub-Signature-256")):
        log.warning("Rejected POST with invalid signature")
        return Response(status_code=403)

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return {"status": "ignored (bad json)"}

    if data.get("object") != "whatsapp_business_account":
        return {"status": "ignored (not whatsapp)"}

    for entry in data.get("entry", []):
        for change in entry.get("changes", []):
            val = change.get("value", {})
            bot_phone_id = (val.get("metadata") or {}).get("phone_number_id") or PHONE_NUMBER_ID

            for msg in val.get("messages", []):
                if msg.get("type") != "text":
                    continue

                sender_phone = msg.get("from")
                message_id = msg.get("id", "")
                message_text = (msg.get("text") or {}).get("body", "").strip()
                log.info("[WA] %s: %s", sender_phone, message_text)

                task = asyncio.create_task(_dispatch_safe(sender_phone, bot_phone_id, message_id, message_text))
                task.add_done_callback(_log_task_result)

    return {"status": "ok"}

def _log_task_result(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    if (exc := task.exception()) is not None:
        log.error("Background dispatch crashed: %r", exc, exc_info=exc)


# ------------------------------------------------------------------ AI Generators
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

def evaluate_intent(user_msg: str) -> dict:
    prompt = f"""You are Ani from Livelyher. The user is in a consultation funnel.
User just said: "{user_msg}"

Task:
1. Are they generally agreeing to proceed, answering "yes/ok", or saying "I am here"? (Return is_valid: true)
2. If they are asking an out-of-context question or complaining, return is_valid: false, and write a polite, short Roman Urdu reply addressing their concern. 
NEVER USE MARKDOWN (no *, #, -, etc). Plain text only.

Return ONLY pure JSON in this format: 
{{"is_valid": true/false, "reply": "string or null"}}"""
    
    response = groq().chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    return json.loads(response.choices[0].message.content)

def generate_medical_pitch(a1, a2, a3) -> str:
    prompt = f"""You are a health coach. The user's data: Physical: {a1} | Diet: {a2} | Stress: {a3}

Write exactly ONE paragraph in simple English using this exact structure (fill in the brackets with scientific but simple explanations based on their data):
"Based on the situation you described [mention specific situation causing effect], it suggests you might be dealing with [their medical conditions like insulin sensitivity, PCOS, or obesity]. In this condition, [simply scientifically explain what happens in it relating to them] which is why [their pain or problem they told they are suffering]."

CONSTRAINTS:
- Write strictly in simple, conversational English.
- DO NOT add extra greetings or endings. Keep the answer to a single paragraph.
- STRICTLY NO MARKDOWN (no asterisks, no hashes, no bullet points). Plain text only."""
    response = groq().chat.completions.create(model=GROQ_MODEL, messages=[{"role": "user", "content": prompt}])
    return response.choices[0].message.content.strip().replace("*", "").replace("#", "")

def generate_fear_pitch(a1, a2, a3) -> str:
    prompt = f"""You are a health coach. User's data: Physical: {a1} | Diet: {a2} | Stress: {a3}

Write exactly ONE paragraph in simple English using this exact structure to install a realistic fear element:
"In the coming months or a year, leaving this untreated can lead to [tell what happens if we leave it untreated like reaching 90+ weight, diabetes, losing body shape, PCOS, etc., based on their specific problem], but don't worry, Insha'Allah we will completely fix it in a few weeks!"

CONSTRAINTS:
- Do not make the fear paralyzing, just realistic.
- Write strictly in simple, conversational English.
- DO NOT add extra greetings or endings. Keep the answer to a single paragraph.
- STRICTLY NO MARKDOWN (no asterisks, no hashes, no bullet points). Plain text only."""
    response = groq().chat.completions.create(model=GROQ_MODEL, messages=[{"role": "user", "content": prompt}])
    return response.choices[0].message.content.strip().replace("*", "").replace("#", "")

def _validate_or_accept(questions: str, message_text: str) -> dict:
    try:
        return validate_answer(questions, message_text)
    except Exception:
        log.exception("Validation crashed — accepting answer to keep funnel moving")
        return {"is_valid": True}


# ------------------------------------------------------------------ Core Logic Flow
async def _dispatch_safe(sender_phone: str, bot_phone_id: str, message_id: str, message_text: str) -> None:
    try:
        await _dispatch(sender_phone, bot_phone_id, message_id, message_text)
    except Exception:
        log.exception("Dispatch failed for %s", sender_phone)


async def _dispatch(sender_phone: str, bot_phone_id: str, message_id: str, message_text: str) -> None:
    
    with _db() as conn:
        if message_id:
            try:
                conn.execute("INSERT INTO seen_messages(message_id) VALUES (?)", (message_id,))
            except sqlite3.IntegrityError:
                log.info("Duplicate delivery of %s — skipping", message_id)
                return

        cursor = conn.execute("SELECT chat_stage, answers_1, answers_2, answers_3 FROM users WHERE user_phone = ?", (sender_phone,))
        row = cursor.fetchone()

    async def send(text: str):
        await asyncio.to_thread(meta.send_whatsapp_text, bot_phone_id, sender_phone, text)
        log.info("[WA] -> %s: %.80s", sender_phone, text)

    # STATE 0: NEW USER (30 SECOND DELAY)
    if not row:
        with _db() as conn:
            conn.execute("INSERT INTO users (user_phone, bot_phone_id, chat_stage) VALUES (?, ?, 1)", (sender_phone, bot_phone_id))
        
        log.info("--> [NEW USER] Delaying 30s before first response to %s", sender_phone)
        await asyncio.sleep(30)
        await send(MSG_1)
        return

    stage, a1, a2, a3 = row

    with _db() as conn:
        conn.execute("UPDATE users SET bot_phone_id = ? WHERE user_phone = ?", (bot_phone_id, sender_phone))

    # Smart Intent Checker
    if stage >= 6:
        intent = await asyncio.to_thread(evaluate_intent, message_text)
        if not intent.get("is_valid"):
            log.info("--> [OUT OF BAND] Answering user's question instead of advancing state.")
            await asyncio.sleep(10)
            await send(intent.get("reply") or "Please confirm you are ready to proceed.")
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
        val = await asyncio.to_thread(_validate_or_accept, SET_1, message_text)
        await asyncio.sleep(10)
        if val.get("is_valid"):
            with _db() as conn:
                conn.execute("UPDATE users SET chat_stage = 3, answers_1 = ? WHERE user_phone = ?", (message_text, sender_phone))
            await send(SET_2)
        else:
            await send(val.get("reply_if_invalid") or INVALID_FALLBACK)

    elif stage == 3:
        val = await asyncio.to_thread(_validate_or_accept, SET_2, message_text)
        await asyncio.sleep(10)
        if val.get("is_valid"):
            with _db() as conn:
                conn.execute("UPDATE users SET chat_stage = 4, answers_2 = ? WHERE user_phone = ?", (message_text, sender_phone))
            await send(SET_3)
        else:
            await send(val.get("reply_if_invalid") or INVALID_FALLBACK)

    elif stage == 4:
        val = await asyncio.to_thread(_validate_or_accept, SET_3, message_text)
        await asyncio.sleep(10)
        if val.get("is_valid"):
            await send(MSG_END)
            
            # --- BYPASSING 30 MIN DELAY FOR TESTING ---
            await asyncio.sleep(12) 
            await send(PITCH_1)
            with _db() as conn:
                # Bypass Stage 5 and jump straight to Stage 6
                conn.execute("UPDATE users SET chat_stage = 6, answers_3 = ? WHERE user_phone = ?", (message_text, sender_phone))
        else:
            await send(val.get("reply_if_invalid") or INVALID_FALLBACK)

    elif stage == 5:
        await asyncio.sleep(10)
        await send(MSG_WAIT)

    elif stage == 6:
        # User replied to PITCH_1 ("I am here")
        msg2 = await asyncio.to_thread(generate_medical_pitch, a1, a2, a3)
        await asyncio.sleep(10)
        await send(msg2)
        await asyncio.sleep(12)
        await send(PITCH_3)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 7 WHERE user_phone = ?", (sender_phone,))

    elif stage == 7:
        # User replied to PITCH_3 ("Getting my point?")
        msg4 = await asyncio.to_thread(generate_fear_pitch, a1, a2, a3)
        await asyncio.sleep(10)
        await send(msg4)
        await asyncio.sleep(10)
        await send(PITCH_5)
        await asyncio.sleep(10)
        await send(PITCH_6)
        await asyncio.sleep(10)
        await send(PITCH_7)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 8 WHERE user_phone = ?", (sender_phone,))

    elif stage == 8:
        # User replied to PITCH_7 ("Exercise suggest karain")
        await asyncio.sleep(10)
        await send(PITCH_8)
        await asyncio.sleep(10)
        await send(PITCH_9)
        await asyncio.sleep(10)
        await send(PITCH_10)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 9 WHERE user_phone = ?", (sender_phone,))

    elif stage == 9:
        # User replied to PITCH_10 ("Seen the video")
        await asyncio.sleep(10)
        await send(PITCH_11)
        await asyncio.sleep(10)
        # Skip PITCH_12, immediately go to PITCH_13
        await send(PITCH_13)
        await asyncio.sleep(10)
        await send(PITCH_14)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 10 WHERE user_phone = ?", (sender_phone,))

    elif stage == 10:
        # User replied to PITCH_14 ("Send questions")
        await asyncio.sleep(10)
        await send(PITCH_15)
        await asyncio.sleep(10)
        await send(PITCH_16)
        with _db() as conn:
            conn.execute("UPDATE users SET chat_stage = 11 WHERE user_phone = ?", (sender_phone,))