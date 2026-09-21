# =============================================================================
# Dockerfile for dbt-runner Cloud Run Job
# Python version and dbt versions are managed in requirements.txt
#
# dbt v2 : le moteur est ecrit en Rust et embarque son adaptateur BigQuery,
# d'ou un requirements-lock.txt passe de ~110 paquets Python a 4 (plus de
# pyarrow / pandas / google-cloud-* ici).
#
# ATTENTION : installe par pip, `dbt` n'est PAS un binaire autonome. C'est une
# extension CPython (dbt/_core.abi3.so, ~400 Mo) appelee par un petit script
# Python. Python reste donc requis A L'EXECUTION, et la wheel declare
# Requires-Python >=3.11 : d'ou python:3.11-slim, qui n'est pas qu'un fournisseur
# de pip. pip est la methode d'installation documentee par dbt Labs pour v2.
#
# Un binaire vraiment autonome existe (script install.sh, image sans Python,
# ~130 Mo de moins). Non retenu : on reste sur la voie standard.
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
