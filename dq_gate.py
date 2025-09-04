import os, sys
from google.cloud import bigquery

PROJECT_ID = os.environ.get("PROJECT_ID", "drilldown-case-study")
MAX_STALE_DAYS = int(os.environ.get("FRESHNESS_MAX_DAYS", "365"))  # configurable

client = bigquery.Client(project=PROJECT_ID)

def cell(sql, field):
    return list(client.query(sql).result())[0][field]

def main():
    problems = []

    # Use the most relevant business date we have: latest of invoice, paid, due
    latest_days_sql = f"""
    SELECT DATE_DIFF(
             CURRENT_DATE(),
             MAX(GREATEST(
               COALESCE(invoice_date, DATE '1900-01-01'),
               COALESCE(paid_date,    DATE '1900-01-01'),
               COALESCE(due_date,     DATE '1900-01-01')
             )),
             DAY
           ) AS days_since_latest
    FROM `{PROJECT_ID}.drilldown_core.fact_sales`
    """
    days_since_latest = cell(latest_days_sql, "days_since_latest")

    if days_since_latest is None:
        problems.append("stale data: could not compute latest business date")
    elif int(days_since_latest) > MAX_STALE_DAYS:
        problems.append(f"stale data: days_since_latest={days_since_latest} > {MAX_STALE_DAYS}")

    negatives_sql = f"""
    SELECT COUNTIF(line_amount < 0) AS negative_lines
    FROM `{PROJECT_ID}.drilldown_core.fact_sales`
    """
    negative_lines = cell(negatives_sql, "negative_lines")
    if negative_lines and int(negative_lines) > 0:
        problems.append(f"negative line amounts: {negative_lines}")

    if problems:
        print("DQ FAILED:", " | ".join(problems))
        sys.exit(1)
    print("DQ OK")

if __name__ == "__main__":
    main()
