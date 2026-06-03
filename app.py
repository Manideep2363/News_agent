from datetime import datetime, timedelta, timezone
import logging
import os

import psycopg2
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr

from news_agent import run_for_user, setup_logging

app = FastAPI(title="News Agent API", version="1.0.0")
security = HTTPBearer()
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
load_dotenv()
logger = logging.getLogger("news-agent-api")
scheduler = BackgroundScheduler(timezone=os.getenv("TIMEZONE", "Asia/Kolkata"))

JWT_SECRET = os.getenv("JWT_SECRET", "change-me-in-env")
JWT_ALGO = "HS256"
JWT_EXP_HOURS = int(os.getenv("JWT_EXP_HOURS", "24"))

origins = os.getenv("CORS_ORIGINS", "http://localhost:4200,http://127.0.0.1:4200")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in origins.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SignUpRequest(BaseModel):
    email: EmailStr
    password: str
    topic: str
    delivery_time: str


class SignInRequest(BaseModel):
    email: EmailStr
    password: str


class UpdateProfileRequest(BaseModel):
    topic: str
    delivery_time: str


class ProfileResponse(BaseModel):
    email: EmailStr
    topic: str
    delivery_time: str


def get_db_connection():
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        return psycopg2.connect(database_url)
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "news_agent"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )


def normalize_time(value: str) -> str:
    value = value.strip()
    for fmt in ["%H:%M", "%I:%M %p", "%I %p", "%H"]:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%H:%M")
        except ValueError:
            continue
    raise HTTPException(status_code=422, detail="Use valid time like 08:00 or 08:00 AM")


def validate_password(password: str) -> None:
    if len(password) < 6:
        raise HTTPException(status_code=422, detail="Password must be at least 6 chars")
    if len(password.encode("utf-8")) > 256:
        raise HTTPException(status_code=422, detail="Password is too long")


def init_db() -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    email TEXT PRIMARY KEY,
                    topic TEXT NOT NULL,
                    delivery_time VARCHAR(5) NOT NULL,
                    password_hash TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT;")
        conn.commit()


def fetch_all_users():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT topic, email, delivery_time FROM users")
            rows = cur.fetchall()
    return [{"topic": r[0], "email": r[1], "delivery_time": r[2]} for r in rows]


def schedule_user_digest(user: dict) -> None:
    hh, mm = user["delivery_time"].split(":")
    job_id = f"digest_{user['email'].lower()}"
    scheduler.add_job(
        run_for_user,
        "cron",
        args=[user],
        hour=int(hh),
        minute=int(mm),
        id=job_id,
        replace_existing=True,
    )
    logger.info(
        "Scheduled automatic digest | email=%s | topic=%s | time=%s",
        user["email"],
        user["topic"],
        user["delivery_time"],
    )


def schedule_all_users() -> None:
    users = fetch_all_users()
    for user in users:
        schedule_user_digest(user)
    logger.info("Scheduler sync complete. Active users=%s", len(users))


def create_token(email: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(hours=JWT_EXP_HOURS)
    return jwt.encode({"sub": email, "exp": exp}, JWT_SECRET, algorithm=JWT_ALGO)


def decode_token(token: str) -> str:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        email = payload.get("sub")
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")
        return str(email)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def get_current_email(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    return decode_token(credentials.credentials)


@app.on_event("startup")
def startup() -> None:
    setup_logging()
    init_db()
    if not scheduler.running:
        scheduler.start()
    schedule_all_users()
    logger.info("API startup complete. Digest logging and scheduler enabled.")


@app.on_event("shutdown")
def shutdown() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shutdown complete.")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/auth/signup")
def signup(payload: SignUpRequest):
    validate_password(payload.password)
    topic = payload.topic.strip()
    if not topic:
        raise HTTPException(status_code=422, detail="Topic is required")

    delivery_time = normalize_time(payload.delivery_time)
    password_hash = pwd_context.hash(payload.password)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT email FROM users WHERE email = %s", (payload.email.lower(),))
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Email already registered")
            cur.execute(
                """
                INSERT INTO users (email, topic, delivery_time, password_hash)
                VALUES (%s, %s, %s, %s)
                """,
                (payload.email.lower(), topic, delivery_time, password_hash),
            )
        conn.commit()

    schedule_user_digest(
        {"topic": topic, "email": payload.email.lower(), "delivery_time": delivery_time}
    )
    return {"message": "User created"}


@app.post("/auth/signin")
def signin(payload: SignInRequest):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT email, password_hash FROM users WHERE email = %s",
                (payload.email.lower(),),
            )
            row = cur.fetchone()

    if not row or not row[1] or not pwd_context.verify(payload.password, row[1]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return {"access_token": create_token(row[0])}


@app.get("/users/me", response_model=ProfileResponse)
def get_me(current_email: str = Depends(get_current_email)):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT email, topic, delivery_time FROM users WHERE email = %s",
                (current_email,),
            )
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    return {"email": row[0], "topic": row[1], "delivery_time": row[2]}


@app.put("/users/me")
def update_me(payload: UpdateProfileRequest, current_email: str = Depends(get_current_email)):
    topic = payload.topic.strip()
    if not topic:
        raise HTTPException(status_code=422, detail="Topic is required")
    delivery_time = normalize_time(payload.delivery_time)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE users
                SET topic = %s, delivery_time = %s, updated_at = NOW()
                WHERE email = %s
                """,
                (topic, delivery_time, current_email),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="User not found")
        conn.commit()

    schedule_user_digest(
        {"topic": topic, "email": current_email, "delivery_time": delivery_time}
    )
    return {"message": "Profile updated"}


@app.post("/digest/send-now")
def send_digest_now(current_email: str = Depends(get_current_email)):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT topic, email, delivery_time FROM users WHERE email = %s",
                (current_email,),
            )
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    user = {"topic": row[0], "email": row[1], "delivery_time": row[2]}
    logger.info("Manual digest trigger requested for %s (%s)", user["email"], user["topic"])
    run_for_user(user)
    logger.info("Manual digest trigger finished for %s", user["email"])
    return {"message": "Digest run completed. Check logs/news_agent.log for full mail logs."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)