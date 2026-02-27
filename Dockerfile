# =============================================================================
# Dockerfile for dbt-runner Cloud Run Job
# Matches VM environment: Python 3.11 + dbt-bigquery 1.11.0
# =============================================================================
FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-bigquery==1.11.0

WORKDIR /app

# Copy dbt project files
COPY dbt_project.yml packages.yml profiles.yml ./
COPY models/ models/
COPY snapshots/ snapshots/
COPY data/ data/
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
