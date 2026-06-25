# =============================================================================
# Dockerfile for dbt-runner Cloud Run Job
# Python version and dbt versions are managed in requirements.txt
# =============================================================================
FROM python:3.11-slim

# git is required by `dbt deps` to install git-sourced packages (e.g. dbt_orphan).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-lock.txt ./
RUN pip install --no-cache-dir -r requirements-lock.txt

WORKDIR /app

COPY dbt_project.yml packages.yml profiles.yml selectors.yml ./
COPY models/ models/
COPY snapshots/ snapshots/
COPY data/ data/
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
