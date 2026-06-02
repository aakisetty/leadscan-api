# LeadScan AI — Render Deployment

Turns `{ industry, location }` into enriched GHL-ready leads automatically.

## Deploy to Render in 5 steps

### 1. Push to GitHub

```bash
cd leadscan_api
git init
git add .
git commit -m "Initial LeadScan API"
# Create a new repo on GitHub, then:
git remote add origin https://github.com/YOUR_USER/leadscan-api.git
git push -u origin main
```

### 2. Create a new Web Service on Render

1. Go to [render.com](https://render.com) → **New → Web Service**
2. Connect your GitHub repo
3. Render auto-detects `render.yaml` — confirm the settings:
   - **Name:** leadscan-api
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn api:app --host 0.0.0.0 --port $PORT --workers 1`
   - **Plan:** Starter ($7/month) for always-on, or Free for low-volume

### 3. Set environment variables

In the Render dashboard → **Environment** → add each key:

| Variable | Value |
|---|---|
| `GOOGLE_PLACES_API_KEY` | Your Google Places API key |
| `PAGESPEED_API_KEY` | Your PageSpeed Insights API key |
| `ANTHROPIC_API_KEY` | Your Anthropic API key |
| `GHL_API_KEY` | Your GHL Location API key |
| `GHL_LOCATION_ID` | Your GHL sub-account Location ID |
| `GHL_PIPELINE_ID` | LeadScan AI pipeline ID |
| `GHL_STAGE_NO_WEBSITE` | Stage ID |
| `GHL_STAGE_WEBSITE_NO_ADS` | Stage ID |
| `GHL_STAGE_NEEDS_GBP_ADS` | Stage ID |
| `GHL_STAGE_NURTURE` | Stage ID |
| `GHL_WORKFLOW_ID` | *(optional)* Outreach workflow ID |

### 4. Deploy

Click **Deploy**. Render will install dependencies and start the server. Takes ~2 minutes.

### 5. Test it

```bash
# Health check
curl https://leadscan-api.onrender.com/health

# Trigger a run
curl -X POST https://leadscan-api.onrender.com/run \
  -H "Content-Type: application/json" \
  -d '{"industry": "plumbers", "location": "Sydney", "max_pages": 1}'

# Response: {"job_id": "abc-123", "status": "queued", "status_url": "/jobs/abc-123"}

# Check progress
curl https://leadscan-api.onrender.com/jobs/abc-123

# Get results (once completed)
curl https://leadscan-api.onrender.com/results/abc-123
```

Or just open `https://leadscan-api.onrender.com` in a browser for the dashboard.

---

## Local development

```bash
cp .env.example .env
# Fill in your API keys in .env

pip install -r requirements.txt
uvicorn api:app --reload --port 8000
# Open http://localhost:8000
```

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Web dashboard |
| `GET` | `/health` | Health check |
| `POST` | `/run` | Start a pipeline run |
| `GET` | `/jobs/{id}` | Job status + progress log |
| `GET` | `/results/{id}` | Completed leads JSON |
| `GET` | `/jobs` | List all jobs |
| `GET` | `/docs` | Auto-generated Swagger UI |

### POST /run — Request body

```json
{
  "industry":   "plumbers",
  "location":   "Sydney",
  "max_pages":  1,
  "region":     "AU",
  "skip_dedup": true,
  "skip_crm":   false
}
```

- `max_pages` — 1 page ≈ 20 businesses ≈ $0.34 in Places API costs
- `skip_dedup` — set `false` once GHL is configured; `true` for audit-only
- `skip_crm` — set `true` to run Agents 1-3 only (no GHL write)

---

## Cost per run

| Component | Cost per 20-business run |
|---|---|
| Google Places API | ~$0.34 |
| PageSpeed Insights | Free (25K calls/day) |
| Claude Haiku (gap summaries) | ~$0.01 |
| GHL API | Free |
| **Total** | **~$0.35** |

---

## File structure

```
leadscan_api/
├── api.py            FastAPI app — endpoints + dashboard
├── pipeline.py       Pipeline orchestrator (chains Agent 1→2→3→4)
├── agents/
│   ├── scraper.py    Agent 1 — Google Places scraper
│   ├── auditor.py    Agent 2 — Website/SEO/PageSpeed/GBP/Meta Ads audit
│   ├── summariser.py Agent 3 — Claude gap summary generator
│   └── crm_writer.py Agent 4 — GHL contact + opportunity upsert
├── requirements.txt
├── render.yaml       Render service configuration
├── .env.example      Template for local development
└── .gitignore
```
