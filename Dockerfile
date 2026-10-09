# =============================================================================
# Image dbt-runner (Cloud Run Job). Version de dbt : requirements-lock.txt.
#
# Le paquet `dbt` (v2) est une extension CPython, pas un binaire autonome :
# Python >= 3.11 est requis à l'exécution, d'où python:3.11-slim. pip est la
# méthode d'installation documentée par dbt Labs.
# =============================================================================
FROM python:3.11-slim

# git : requis par `dbt deps` pour les paquets installés depuis git (dbt_orphan).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-lock.txt ./
RUN pip install --no-cache-dir -r requirements-lock.txt

WORKDIR /app

# Paquets dbt installés à la construction, figés par package-lock.yml : une
# exécution ne dépend ni de hub.getdbt.com ni de github.com.
COPY dbt_project.yml packages.yml package-lock.yml profiles.yml selectors.yml ./
RUN dbt deps

# Copier TOUS les chemins déclarés dans dbt_project.yml (models, tests, macros,
# seeds, snapshots) : un chemin absent n'est pas signalé par dbt, il est
# simplement ignoré (un test-path manquant = 0 test singulier en prod).
COPY models/ models/
COPY macros/ macros/
COPY snapshots/ snapshots/
COPY data/ data/
COPY tests/ tests/
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
