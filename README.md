# My Notebook + Streamlit Project

This project combines exploratory data analysis in Jupyter Notebooks with a deployable interactive app built using Streamlit. It is designed for simplicity, fast iteration, and clarity.

## 📁 Project Structure

## Data Explorer (read-only Streamlit app)

A sidecar UI over the database and the current export — it writes nothing;
the notebook remains the only thing that updates the DB.

```
python -m streamlit run app.py
```

Tabs: **This Week** (rankings, model-vs-market scatter, value-vs-salary),
**SG Rankings** (recency-weighted strokes-gained form with sparklines),
**Player Detail** (SG-per-round history, recent results, course history).
