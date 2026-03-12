# Mine Risk MVP Demo App

## Run (Local)
From the repo root:

```
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/mine_risk
streamlit run app/streamlit_app.py
```

## Database Configuration
The app reads `DATABASE_URL` in this order:
- Streamlit secrets: `st.secrets["DATABASE_URL"]`
- Environment variable: `DATABASE_URL`

## Deployment Notes (Streamlit Community Cloud)
- Entrypoint: `app/streamlit_app.py`
- Dependencies: `requirements-demo.txt`
- Secrets: add `DATABASE_URL` in Streamlit Cloud secrets
- Example secrets file: `.streamlit/secrets.toml.example`

## Pages
- Executive Overview
- Top-Risk Mines
- Mine Detail Drill-Through
- Backtest Performance
- Governance & Freshness
