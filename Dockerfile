# =============================================================================
# Dockerfile for dbt-runner Cloud Run Job
# Uses official dbt Labs image — update minor version intentionally
# =============================================================================
FROM ghcr.io/dbt-labs/dbt-bigquery:1.11.1

WORKDIR /app

COPY dbt_project.yml packages.yml profiles.yml ./
COPY models/ models/
COPY snapshots/ snapshots/
COPY data/ data/
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
