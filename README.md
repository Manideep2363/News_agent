# News Agent MVP

A practical MVP that:
- stores users (`topic`, `email`, `delivery_time`) in PostgreSQL
- runs daily with APScheduler + LangGraph
- fetches news from NewsData + RSS feeds
- deduplicates, ranks, summarizes with Groq
- sends newsletter email via Gmail API

## Project structure
- `news_agent.py` - scheduled digest pipeline
- `app.py` - FastAPI auth/profile backend
- `frontend/` - Angular frontend (signup/signin/profile)
- `requirements.txt` - Python dependencies
- `.env` - runtime config

## 1) Python setup
```bash
pip install -r requirements.txt
```

## 2) Environment variables (`.env`)
```env
GROQ_API_KEY=your_groq_key
MODEL=llama-3.3-70b-versatile
NEWSDATA_API_KEY=your_newsdata_key
TIMEZONE=Asia/Kolkata
TOP_N=5
RANK_CANDIDATES=12
SCORE_THRESHOLD=6.5
GMAIL_CREDENTIALS_FILE=email_credentials.json
GMAIL_SENDER=me

DATABASE_URL=postgresql://postgres:password@localhost:5432/news_agent
# OR use PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD

JWT_SECRET=replace_with_secure_random_value
JWT_EXP_HOURS=24
CORS_ORIGINS=http://localhost:4200,http://127.0.0.1:4200
```

## 3) Run FastAPI backend
```bash
uvicorn app:app --reload
```

Endpoints:
- `POST /auth/signup`
- `POST /auth/signin`
- `GET /users/me` (Bearer token)
- `PUT /users/me` (Bearer token)

## 4) Run Angular frontend
```bash
cd frontend
npm install
npm start
```
Open `http://localhost:4200`.

## 5) Register scheduler user (CLI path)
```bash
python news_agent.py --register
```

## 6) Test digest now
```bash
python news_agent.py --run-once
```

## 7) Start daily scheduler
```bash
python news_agent.py
```
