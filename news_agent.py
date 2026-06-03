import base64
import difflib
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, TypedDict
from urllib.parse import urlparse

import feedparser
import psycopg2
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from groq import Groq
from langgraph.graph import END, START, StateGraph

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
TOKEN_FILE = Path("token.pickle")
DEFAULT_RSS_FEEDS = [
    # "https://techcrunch.com/feed/",
    # "https://www.theverge.com/rss/index.xml",
    # "https://feeds.arstechnica.com/arstechnica/technology-lab",
    # "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    # "https://feeds.marketwatch.com/marketwatch/topstories/",
]


class AgentState(TypedDict):
    user: Dict[str, Any]
    topic: str
    newsdata_articles: List[Dict[str, Any]]
    rss_articles: List[Dict[str, Any]]
    all_articles: List[Dict[str, Any]]
    deduped_articles: List[Dict[str, Any]]
    ranked_articles: List[Dict[str, Any]]
    top_articles: List[Dict[str, Any]]
    newsletter_html: str
    sent_ok: bool


@dataclass
class UserConfig:
    topic: str
    email: str
    delivery_time: str


def setup_logging() -> None:
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    log_file = os.getenv("LOG_FILE", str(logs_dir / "news_agent.log"))
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    handlers = [
        logging.StreamHandler(),
        RotatingFileHandler(log_file, maxBytes=2_000_000, backupCount=5, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=handlers,
        force=True,
    )


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


def init_db() -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    email TEXT PRIMARY KEY,
                    topic TEXT NOT NULL,
                    delivery_time VARCHAR(5) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        conn.commit()


def load_users() -> List[Dict[str, str]]:
    init_db()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT topic, email, delivery_time
                FROM users
                ORDER BY email ASC;
                """
            )
            rows = cur.fetchall()
    return [
        {"topic": topic, "email": email, "delivery_time": delivery_time}
        for topic, email, delivery_time in rows
    ]


def upsert_user(user: UserConfig) -> None:
    init_db()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (topic, email, delivery_time)
                VALUES (%s, %s, %s)
                ON CONFLICT (email)
                DO UPDATE SET
                    topic = EXCLUDED.topic,
                    delivery_time = EXCLUDED.delivery_time,
                    updated_at = NOW();
                """,
                (user.topic, user.email, user.delivery_time),
            )
        conn.commit()


def normalize_time(value: str) -> str:
    value = value.strip()
    for fmt in ["%I:%M %p", "%I %p", "%H:%M", "%H"]:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%H:%M")
        except ValueError:
            continue
    raise ValueError("Use a valid time format like 08:00 AM or 20:30")


def register_user_interactive() -> None:
    print("Enter daily digest setup:")
    topic = input("Topic: ").strip()
    email = input("Email: ").strip()
    raw_time = input("Delivery time (e.g. 08:00 AM): ").strip()

    if not topic:
        raise ValueError("Topic cannot be empty.")
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        raise ValueError("Invalid email format.")

    delivery_time = normalize_time(raw_time)

    upsert_user(UserConfig(topic=topic, email=email, delivery_time=delivery_time))
    print(f"Saved user config for {email} at {delivery_time}.")


def fetch_newsdata(topic: str, limit: int = 30) -> List[Dict[str, Any]]:
    api_key = os.getenv("NEWSDATA_API_KEY")
    if not api_key:
        logging.warning("NEWSDATA_API_KEY missing. Skipping NewsData source.")
        return []

    url = "https://newsdata.io/api/1/news"
    size = min(max(limit, 10), 50)
    params = {
        "apikey": api_key,
        "q": topic,
        "language": "en",
        "size": size,
    }

    try:
        resp = requests.get(url, params=params, timeout=25)
        if resp.status_code == 422:
            # Some keys/plans reject certain filters (often `size`).
            fallback_params = dict(params)
            fallback_params.pop("size", None)
            resp = requests.get(url, params=fallback_params, timeout=25)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        detail = ""
        if "resp" in locals():
            detail = f" | status={resp.status_code} body={resp.text[:300]}"
        logging.exception("NewsData fetch failed: %s%s", exc, detail)
        return []

    articles = []
    for item in payload.get("results", []):
        articles.append(
            {
                "title": item.get("title") or "Untitled",
                "link": item.get("link") or "",
                "source": item.get("source_id") or "NewsData",
                "published": item.get("pubDate") or "",
                "summary": item.get("description") or "",
            }
        )
    return articles


def fetch_rss(topic: str, feeds: List[str], max_per_feed: int = 20) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    topic_words = set(w.lower() for w in re.findall(r"\w+", topic))

    all_rows: List[Dict[str, Any]] = []
    for feed_url in feeds:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries[:max_per_feed]:
            title = getattr(entry, "title", "")
            summary = getattr(entry, "summary", "")
            text = f"{title} {summary}".lower()
            row = {
                "title": title or "Untitled",
                "link": getattr(entry, "link", ""),
                "source": getattr(feed.feed, "title", urlparse(feed_url).netloc),
                "published": getattr(entry, "published", ""),
                "summary": re.sub("<[^<]+?>", "", summary)[:500],
            }
            all_rows.append(row)

            # Keep it simple but practical: fuzzy topic filtering by keyword overlap.
            if topic_words and not any(word in text for word in topic_words):
                continue

            rows.append(row)
    if not rows:
        # If strict topic match yields nothing, return recent mixed headlines.
        return all_rows[: max_per_feed * max(1, len(feeds))]
    return rows


def article_key(article: Dict[str, Any]) -> str:
    title = article.get("title", "").strip().lower()
    # Remove punctuation/symbol variants so syndicated headlines collapse.
    title = re.sub(r"[^a-z0-9\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    words = title.split()
    # Skip boilerplate token noise and keep a stable fingerprint.
    stop = {"the", "a", "an", "to", "for", "of", "and", "in", "on", "with", "new", "update"}
    core_words = [w for w in words if w not in stop]
    core = " ".join(core_words[:16])
    if len(core) < 12:
        # Fallback when title is too short or noisy.
        link = article.get("link", "").strip().lower()
        domain = urlparse(link).netloc.replace("www.", "")
        core = f"{title}|{domain}"
    return hashlib.sha1(core.encode("utf-8")).hexdigest()


def deduplicate_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    kept_titles: List[str] = []
    for article in articles:
        key = article_key(article)
        if key in seen:
            continue
        title = re.sub(r"\s+", " ", article.get("title", "").strip().lower())
        near_dup = any(difflib.SequenceMatcher(None, title, t).ratio() >= 0.92 for t in kept_titles)
        if near_dup:
            continue
        seen.add(key)
        out.append(article)
        kept_titles.append(title)
    return out


def get_groq_client() -> Groq | None:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        logging.warning("GROQ_API_KEY missing. LLM ranking/summarization disabled.")
        return None
    return Groq(api_key=api_key)


def rank_articles_with_groq(topic: str, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    client = get_groq_client()
    if not client:
        for a in articles:
            a["score"] = 5.0
        return articles

    model = os.getenv("MODEL", "llama-3.3-70b-versatile")
    # Pre-rank cheaply so we only ask Groq to score a smaller candidate set.
    for art in articles:
        text = f"{art.get('title', '')} {art.get('summary', '')}".lower()
        overlap = sum(1 for w in re.findall(r"\w+", topic.lower()) if w in text)
        art["score"] = 4.5 + min(2.5, overlap * 0.6)

    max_candidates = int(os.getenv("RANK_CANDIDATES", "12"))
    candidates = sorted(articles, key=lambda x: x.get("score", 0), reverse=True)[:max_candidates]

    lines = []
    for idx, art in enumerate(candidates, start=1):
        lines.append(
            f"{idx}. Title: {art.get('title', '')}\n"
            f"   Summary: {art.get('summary', '')[:400]}"
        )
    prompt = (
        "You are scoring news relevance. Return strict JSON object only.\n"
        "Output format: {\"scores\": {\"1\": 8.6, \"2\": 7.1}}\n"
        "Score each item 1-10 based on relevance, importance, and impact for the topic.\n"
        f"Topic: {topic}\n\n"
        "Articles:\n"
        + "\n".join(lines)
    )

    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content.strip()
        data = json.loads(raw)
        scores = data.get("scores", {})
        for idx, art in enumerate(candidates, start=1):
            value = scores.get(str(idx), art.get("score", 5.0))
            art["score"] = max(1.0, min(10.0, float(value)))
    except Exception:
        pass

    ranked = sorted(articles, key=lambda x: x.get("score", 0), reverse=True)
    return ranked


def summarize_article(topic: str, article: Dict[str, Any]) -> str:
    client = get_groq_client()
    if not client:
        return article.get("summary", "No summary available.")[:280]

    model = os.getenv("MODEL", "llama-3.3-70b-versatile")
    prompt = (
        "Summarize this article in 3 bullet points, professional tone, max 100 words total.\n"
        f"Topic: {topic}\n"
        f"Title: {article.get('title', '')}\n"
        f"Source summary: {article.get('summary', '')[:1200]}"
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return article.get("summary", "No summary available.")[:280]


def build_newsletter_html(topic: str, articles: List[Dict[str, Any]]) -> str:
    if not articles:
        date_label = datetime.now().strftime("%Y-%m-%d")
        return f"""
        <html>
          <body style=\"font-family:Arial,sans-serif;line-height:1.5;\">
            <h2>Daily {topic} Digest</h2>
            <p>Date: {date_label}</p>
            <hr />
            <p>No high-relevance articles found today for this topic.</p>
          </body>
        </html>
        """

    sections = []
    for i, art in enumerate(articles, start=1):
        summary = art.get("llm_summary", "")
        score = art.get("score", "-")
        published = art.get("published", "") or "Date unavailable"
        sections.append(
            f"""
            <div style=\"margin-bottom:20px;\">
              <h3 style=\"margin-bottom:8px;\">{i}. {art.get('title', 'Untitled')}</h3>
              <p style=\"margin:4px 0;color:#555;\">Source: {art.get('source', 'Unknown')} | Date: {published} | Score: {score}</p>
              <pre style=\"white-space:pre-wrap;font-family:Arial,sans-serif;\">{summary}</pre>
              <a href=\"{art.get('link', '#')}\" target=\"_blank\">Read More</a>
            </div>
            """
        )

    date_label = datetime.now().strftime("%Y-%m-%d")
    return f"""
    <html>
      <body style=\"font-family:Arial,sans-serif;line-height:1.5;\">
        <h2>Daily {topic} Digest</h2>
        <p>Date: {date_label}</p>
        <hr />
        {''.join(sections)}
      </body>
    </html>
    """


def load_gmail_service() -> Any:
    creds = None

    if TOKEN_FILE.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
        except Exception:
            logging.warning("Existing token file is not JSON. Re-authentication required.")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            cred_path = os.getenv("GMAIL_CREDENTIALS_FILE", "email_credentials.json")
            flow = InstalledAppFlow.from_client_secrets_file(cred_path, SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)


def send_email_via_gmail(to_email: str, subject: str, html_body: str) -> None:
    sender = os.getenv("GMAIL_SENDER", "me")
    logging.info("Preparing email send | to=%s | subject=%s", to_email, subject)
    service = load_gmail_service()

    message = MIMEText(html_body, "html")
    message["to"] = to_email
    message["subject"] = subject

    encoded = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    body = {"raw": encoded}
    response = service.users().messages().send(userId=sender, body=body).execute()
    logging.info("Email sent successfully | to=%s | message_id=%s", to_email, response.get("id", "unknown"))


def node_fetch_newsdata(state: AgentState) -> AgentState:
    state["newsdata_articles"] = fetch_newsdata(state["topic"], limit=35)
    logging.info("Fetched NewsData articles=%s for topic=%s", len(state["newsdata_articles"]), state["topic"])
    return state


def node_fetch_rss(state: AgentState) -> AgentState:
    state["rss_articles"] = fetch_rss(state["topic"], DEFAULT_RSS_FEEDS, max_per_feed=20)
    logging.info("Fetched RSS articles=%s for topic=%s", len(state["rss_articles"]), state["topic"])
    return state


def node_merge(state: AgentState) -> AgentState:
    state["all_articles"] = state.get("newsdata_articles", []) + state.get("rss_articles", [])
    logging.info("Merged total articles=%s", len(state["all_articles"]))
    return state


def node_dedup(state: AgentState) -> AgentState:
    state["deduped_articles"] = deduplicate_articles(state.get("all_articles", []))
    logging.info("Deduplicated articles=%s", len(state["deduped_articles"]))
    return state


def node_rank(state: AgentState) -> AgentState:
    state["ranked_articles"] = rank_articles_with_groq(state["topic"], state.get("deduped_articles", []))
    logging.info("Ranked candidate articles=%s", len(state["ranked_articles"]))
    return state


def node_select_and_summarize(state: AgentState) -> AgentState:
    top_n = int(os.getenv("TOP_N", "5"))
    min_score = float(os.getenv("SCORE_THRESHOLD", "6.5"))
    ranked = state.get("ranked_articles", [])
    filtered = [a for a in ranked if float(a.get("score", 0)) >= min_score]
    if not filtered:
        logging.warning(
            "No articles passed SCORE_THRESHOLD=%s for topic=%s",
            min_score,
            state["topic"],
        )
    selected = filtered[:top_n]
    logging.info(
        "Selected top articles=%s | threshold=%s | requested_top_n=%s",
        len(selected),
        min_score,
        top_n,
    )
    for art in selected:
        art["llm_summary"] = summarize_article(state["topic"], art)
    state["top_articles"] = selected
    return state


def node_generate_newsletter(state: AgentState) -> AgentState:
    if not state.get("top_articles"):
        logging.warning("No top articles found for topic=%s; generating fallback newsletter content.", state["topic"])
    state["newsletter_html"] = build_newsletter_html(state["topic"], state.get("top_articles", []))
    return state


def node_send_email(state: AgentState) -> AgentState:
    subject = f"Daily {state['topic']} Digest"
    try:
        send_email_via_gmail(state["user"]["email"], subject, state.get("newsletter_html", ""))
        state["sent_ok"] = True
    except Exception as exc:
        state["sent_ok"] = False
        logging.exception("Email send failed | to=%s | topic=%s | error=%s", state["user"]["email"], state["topic"], exc)
    return state


def build_graph():
    graph = StateGraph(AgentState)

    graph.add_node("fetch_newsdata", node_fetch_newsdata)
    graph.add_node("fetch_rss", node_fetch_rss)
    graph.add_node("merge", node_merge)
    graph.add_node("dedup", node_dedup)
    graph.add_node("rank", node_rank)
    graph.add_node("select_summarize", node_select_and_summarize)
    graph.add_node("generate_newsletter", node_generate_newsletter)
    graph.add_node("send_email", node_send_email)

    graph.add_edge(START, "fetch_newsdata")
    graph.add_edge("fetch_newsdata", "fetch_rss")
    graph.add_edge("fetch_rss", "merge")
    graph.add_edge("merge", "dedup")
    graph.add_edge("dedup", "rank")
    graph.add_edge("rank", "select_summarize")
    graph.add_edge("select_summarize", "generate_newsletter")
    graph.add_edge("generate_newsletter", "send_email")
    graph.add_edge("send_email", END)

    return graph.compile()


def run_for_user(user: Dict[str, str]) -> None:
    topic = user["topic"]
    logging.info("Running digest for %s (%s)", user["email"], topic)

    app = build_graph()
    state: AgentState = {
        "user": user,
        "topic": topic,
        "newsdata_articles": [],
        "rss_articles": [],
        "all_articles": [],
        "deduped_articles": [],
        "ranked_articles": [],
        "top_articles": [],
        "newsletter_html": "",
        "sent_ok": False,
    }

    result = app.invoke(state)
    logging.info(
        "Done for %s | fetched=%s deduped=%s sent=%s",
        user["email"],
        len(result.get("all_articles", [])),
        len(result.get("deduped_articles", [])),
        result.get("sent_ok", False),
    )


def schedule_all_users() -> None:
    users = load_users()
    if not users:
        logging.warning("No users registered. Run: python news_agent.py --register")
        return

    scheduler = BlockingScheduler(timezone=os.getenv("TIMEZONE", "Asia/Kolkata"))

    for user in users:
        hh, mm = user["delivery_time"].split(":")
        scheduler.add_job(
            run_for_user,
            "cron",
            args=[user],
            hour=int(hh),
            minute=int(mm),
            id=f"digest_{user['email']}",
            replace_existing=True,
        )
        logging.info("Scheduled %s at %s", user["email"], user["delivery_time"])

    logging.info("Scheduler started.")
    scheduler.start()


def run_once_all_users() -> None:
    users = load_users()
    if not users:
        logging.warning("No users registered. Run: python news_agent.py --register")
        return
    for user in users:
        run_for_user(user)


if __name__ == "__main__":
    import argparse

    setup_logging()
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description="Daily Topic News Agent")
    parser.add_argument("--register", action="store_true", help="Register/update a user config")
    parser.add_argument("--run-once", action="store_true", help="Run now for all saved users")
    args = parser.parse_args()

    if args.register:
        register_user_interactive()
    elif args.run_once:
        run_once_all_users()
    else:
        schedule_all_users()



