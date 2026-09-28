#!/usr/bin/env bash
# Met state/manifest.json au niveau de la prod, pour que --defer sache vers quelle
# table prod pointer chaque ref() non construit en local.
#
# Le manifest est déposé par le job `cd` à chaque merge sur master. Ce script ne le
# télécharge QUE s'il a changé (comparaison de la « generation » GCS, ~0,5 s) :
# on peut donc l'appeler aussi souvent qu'on veut.
#
# Appelé automatiquement par les hooks git post-merge (git pull) et post-checkout
# (changement de branche), cf. .pre-commit-config.yaml. À la main :
#   scripts/pull-state.sh          # met à jour si besoin
#   scripts/pull-state.sh --force  # retélécharge quoi qu'il arrive
#
# En mode hook (--hook), il ne fait JAMAIS échouer git : réseau coupé, clé absente
# ou GCS indisponible → un avertissement, et git continue.
set -uo pipefail

MODE=manuel
case "${1:-}" in
  --hook) MODE=hook ;;
  --force) MODE=force ;;
esac

avertir() {
  echo "⚠ manifest prod non mis à jour : $1" >&2
  [ "$MODE" = hook ] && exit 0 || exit 1
}

cd "$(dirname "$0")/.." || exit 0

# post-checkout : ne rien faire sur un checkout de fichier (git checkout -- f.sql),
# seulement sur un changement de branche. pre-commit expose le type de checkout.
if [ "$MODE" = hook ] && [ "${PRE_COMMIT_CHECKOUT_TYPE:-1}" = "0" ]; then
  exit 0
fi

if [ -f .env ]; then set -a; . ./.env; set +a; fi
[ -n "${DBT_BIGQUERY_KEYFILE_DEV:-}" ] || avertir "DBT_BIGQUERY_KEYFILE_DEV absent de .env"
[ -f "$DBT_BIGQUERY_KEYFILE_DEV" ] || avertir "clé introuvable ($DBT_BIGQUERY_KEYFILE_DEV)"

URI="gs://${DBT_STATE_BUCKET:-evs-datastack-dbt-state}/dbt-state/manifest.json"
LOCAL=state/manifest.json
MARQUEUR=state/.generation      # version GCS de la copie locale
export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$DBT_BIGQUERY_KEYFILE_DEV"

distante=$(timeout 20 gcloud storage objects describe "$URI" --format='value(generation)' 2>/dev/null) \
  || avertir "impossible de joindre $URI"
locale=$(cat "$MARQUEUR" 2>/dev/null || true)

if [ "$MODE" != force ] && [ -f "$LOCAL" ] && [ "$distante" = "$locale" ]; then
  [ "$MODE" = hook ] || echo "manifest prod déjà à jour"
  exit 0
fi

mkdir -p state
timeout 60 gcloud storage cp --quiet "$URI" "$LOCAL.tmp" 2>/dev/null \
  || { rm -f "$LOCAL.tmp"; avertir "téléchargement échoué"; }
mv "$LOCAL.tmp" "$LOCAL" && echo "$distante" > "$MARQUEUR"
echo "manifest prod mis à jour (déploiement du $(gcloud storage objects describe "$URI" --format='value(update_time)' 2>/dev/null | cut -c1-16))"
