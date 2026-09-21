# =============================================================================
# Dockerfile for dbt-runner Cloud Run Job
# Python version and dbt versions are managed in requirements.txt
#
# dbt v2 : le paquet `dbt` est un binaire Rust qui embarque son adaptateur
# BigQuery. requirements-lock.txt est passe de ~110 paquets Python a 4, donc
# plus de pyarrow / pandas / google-cloud-* a installer ici. L'image maigrit
# d'autant. python:3.11-slim reste la base : elle ne sert plus qu'a fournir pip
# pour l'installation.
# =============================================================================
FROM python:3.11-slim

# git is required by `dbt deps` to install git-sourced packages (e.g. dbt_orphan).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-lock.txt ./
RUN pip install --no-cache-dir -r requirements-lock.txt

WORKDIR /app

# Ces COPY doivent couvrir TOUS les chemins declares dans dbt_project.yml
# (model-paths, test-paths, macro-paths, seed-paths, snapshot-paths). `tests/`
# manquait : dbt ne signale pas un test-path absent, il collecte simplement 0
# test singulier. Les 6 tests de tests/ etaient donc verts en CI (checkout
# complet) et INEXISTANTS dans tout build de production. Audit 2026-09-16.
COPY dbt_project.yml packages.yml profiles.yml selectors.yml ./
COPY models/ models/
COPY macros/ macros/
COPY snapshots/ snapshots/
COPY data/ data/
COPY tests/ tests/
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
