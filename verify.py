"""End-to-end verification for the Livelyher bot — REAL webhook requests
through the full pipeline (dedupe, state machine, validator, scheduler,
sender) with Meta and Groq mocked out, so it costs nothing and always runs.

    python verify.py

Exits 0 and prints PASS for each check if everything works, 1 otherwise.
"""

from __future__ import annotations

import datetime
import json
import os
import sqlite3
import sys
import tempfile
import time

# ----- isolated test environment BEFORE importing server -------------------
_tmp = tempfile.mkdtemp(prefix="livelyher-verify-")
os.environ["DB_PATH"] = os.path.join(_tmp, "test.db")
os.environ["VERIFY_TOKEN"] = "verify-me-123"
os.environ["WA_ACCESS_TOKEN"] = "fake-token"
os.environ["GROQ_API_KEY"] = "fake-key"

import server  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402  (needs: pip install httpx)

# ----- mocks ----------------------------------------------------------------
SENT: list[tuple[str, str, str]] = []          # (phone_number_id, to, text)
VALIDATION_CALLS = {"count": 0}
PHONE = "923001112223"
BOT_PHONE_ID = "987654321"


def fake_send(self, phone_number_id, to, text):  # noqa: ANN001
    SENT.append((phone_number_id, to, text))


class _FakeChoice:
    def __init__(self, content: str):
        self.message = type("M", (), {"content": content})()


class _FakeCompletions:
    def create(self, model=None, messages=None, response_format=None):  # noqa: ANN001
        prompt = messages[0]["content"]
        if "Return ONLY pure JSON" in prompt:
            VALIDATION_CALLS["count"] += 1
            # First validation attempt rejects, the rest accept:
            if VALIDATION_CALLS["count"] == 1:
                payload = {"is_valid": False,
                           "reply_if_invalid": "MOCK_INVALID_REPLY — please answer the questions"}
            else:
                payload = {"is_valid": True, "reply_if_invalid": None}
            text = json.dumps(payload)
        else:
            text = "FAKE_ANALYSIS: aapka stress aur neend weight loss mein rukawat hai — livelyher plan se theek hoga."
        return type("R", (), {"choices": [_FakeChoice(text)]})()


class _FakeChat:
    completions = _FakeCompletions()


class FakeGroq:
    chat = _FakeChat()


server.meta.send_whatsapp_text = fake_send.__get__(server.meta)
server._groq_client = FakeGroq()

# ----- helpers ---------------------------------------------------------------
client = TestClient(server.app)
RESULTS: list[tuple[str, bool]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, ok))
    print(f"{'PASS ✅' if ok else 'FAIL ❌'}  {name}" + (f" — {detail}" if detail and not ok else ""))


def wa_payload(text: str, msg_id: str, phone: str = PHONE) -> dict:
    return {
        "object": "whatsapp_business_account",
        "entry": [{"changes": [{"value": {
            "metadata": {"phone_number_id": BOT_PHONE_ID},
            "messages": [{"id": msg_id, "from": phone, "type": "text",
                          "text": {"body": text}}],
        }}]}],
    }


def stage_of(phone: str = PHONE) -> int:
    row = server._db().execute(
        "SELECT chat_stage FROM users WHERE user_phone=?", (phone,)).fetchone()
    return row[0] if row else -1


def wait_sends(n: int, timeout: float = 5.0) -> bool:
    t0 = time.time()
    while len(SENT) < n and time.time() - t0 < timeout:
        time.sleep(0.1)
    return len(SENT) >= n


# ----- tests -----------------------------------------------------------------
print("=" * 64)
print("LIVELYHER END-TO-END VERIFICATION")
print("=" * 64)

# 1) webhook handshake
r = client.get("/webhook", params={"hub.mode": "subscribe",
                                   "hub.verify_token": "verify-me-123",
                                   "hub.challenge": "CHA-OK"})
check("webhook verify handshake echoes challenge", r.status_code == 200 and r.text == "CHA-OK")
r = client.get("/webhook", params={"hub.mode": "subscribe",
                                   "hub.verify_token": "wrong",
                                   "hub.challenge": "NOPE"})
check("wrong verify token is rejected (403)", r.status_code == 403)

# 2) health
r = client.get("/")
check("health endpoint reports running", r.status_code == 200 and r.json()["status"] == "running")

# 3) first contact -> MSG_1
client.post("/webhook", json=wa_payload("assalamualaikum", "m1"))
check("first contact: MSG_1 greeting sent", wait_sends(1) and SENT[-1][2] == server.MSG_1)
check("first contact: replied via payload phone_number_id", SENT[-1][0] == BOT_PHONE_ID)
check("stage advanced to 1", stage_of() == 1)

# 4) duplicate delivery is ignored
client.post("/webhook", json=wa_payload("assalamualaikum", "m1"))
time.sleep(0.6)
check("duplicate webhook delivery ignored (dedupe)", len(SENT) == 1)

# 5) greeting reply -> MSG_2 + SET_1
client.post("/webhook", json=wa_payload("wa alaikum salam, theek hoon", "m2"))
check("stage 1 -> MSG_2 + SET_1 sent", wait_sends(3)
      and SENT[-2][2] == server.MSG_2 and SENT[-1][2] == server.SET_1)
check("stage advanced to 2", stage_of() == 2)

# 6) invalid answer (mocked validator says NO once) -> re-ask
client.post("/webhook", json=wa_payload("do you like cricket?", "m3"))
check("invalid answer -> validator's re-ask reply sent",
      wait_sends(4) and "MOCK_INVALID_REPLY" in SENT[-1][2])
check("stage stays 2 on invalid answer", stage_of() == 2)

# 7) real SET_1 answers -> SET_2
client.post("/webhook", json=wa_payload("current 85 target 65, height 5'2, delivery ke baad barha, lower belly", "m4"))
check("valid SET_1 answers -> SET_2 sent", wait_sends(5) and SENT[-1][2] == server.SET_2)
check("stage advanced to 3 + answers_1 stored", stage_of() == 3)

# 8) real SET_2 answers -> SET_3
client.post("/webhook", json=wa_payload("green tea try kiya, breakfast paratha dinner roti, PCOS hai", "m5"))
check("valid SET_2 answers -> SET_3 sent", wait_sends(6) and SENT[-1][2] == server.SET_3)
check("stage advanced to 4", stage_of() == 4)

# 9) real SET_3 answers -> MSG_END + timer scheduled
client.post("/webhook", json=wa_payload("stress 8/10, neend totti hai, choti baat par gussa", "m6"))
check("valid SET_3 answers -> MSG_END sent", wait_sends(7) and SENT[-1][2] == server.MSG_END)
row = server._db().execute(
    "SELECT chat_stage, analysis_send_at FROM users WHERE user_phone=?", (PHONE,)).fetchone()
check("stage 5 set with a 30-min timestamp",
      row[0] == 5 and row[1] > datetime.datetime.now(datetime.timezone.utc).isoformat())

# 10) message while waiting -> holding reply, stage unchanged
client.post("/webhook", json=wa_payload("ok jaldi karna", "m7"))
check("stage 5 waiting -> holding message sent", wait_sends(8) and SENT[-1][2] == server.MSG_WAIT)
check("stage still 5", stage_of() == 5)

# 11) 30 minutes pass -> scheduler sends the AI pitch
server._db().execute(
    "UPDATE users SET analysis_send_at=? WHERE user_phone=?",
    ((datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1)).isoformat(),
     PHONE)).connection.commit()
server.check_scheduled_analyses()
time.sleep(0.5)
check("scheduler sends personalized AI pitch at due time",
      len(SENT) == 9 and "FAKE_ANALYSIS" in SENT[-1][2])
check("stage advanced to 6 after pitch", stage_of() == 6)

# 12) reply after pitch -> closing message
client.post("/webhook", json=wa_payload("thank you so much", "m8"))
check("stage 6 -> closing message sent", wait_sends(10) and SENT[-1][2] == server.MSG_DONE)

# 13) junk inputs don't crash anything
r = client.post("/webhook", json={"object": "page", "entry": []})
check("non-WhatsApp payload ignored cleanly", r.status_code == 200 and "ignored" in r.json()["status"])
r = client.post("/webhook", content=b"not-json")
check("invalid JSON handled without 500", r.status_code == 200)
r = client.post("/webhook", json={"object": "whatsapp_business_account", "entry": [{"changes": [
    {"value": {"metadata": {"phone_number_id": BOT_PHONE_ID}, "statuses": [{"status": "read"}]}}]}]})
check("status-only callbacks (read receipts) handled", r.status_code == 200)

# 14) second lead gets their own independent funnel
client.post("/webhook", json=wa_payload("hi", "x1", phone="923009998887"))
wait_sends(11)
check("second lead handled independently", SENT[-1][1] == "923009998887"
      and SENT[-1][2] == server.MSG_1 and stage_of("923009998887") == 1
      and stage_of(PHONE) == 6)

# ----- summary ---------------------------------------------------------------
print("=" * 64)
passed = sum(ok for _, ok in RESULTS)
print(f"RESULT: {passed}/{len(RESULTS)} checks passed"
      + (" — everything works! 🎉" if passed == len(RESULTS) else ""))
print("=" * 64)
sys.exit(0 if passed == len(RESULTS) else 1)
