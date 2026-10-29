"""Historise une exécution du job dbt-runner et signale ses échecs.

Appelé par entrypoint.sh à la sortie du job, même en échec :
- dépose dans DBT_ARTIFACTS_URI une enveloppe JSON d'une ligne : le contexte de
  l'exécution (job, sélection, codes retour) et les artefacts run_results.json et
  sources.json tels quels. dbt v2 n'écrit pas la sélection dans ses artefacts :
  seul le contexte la porte. Table externe : prod_raw.ext_gcs_dbt_artifacts__runner_execution ;
- écrit une ligne JSON de sévérité ERROR par échec (build ou fraîcheur). Cloud
  Logging ne tire une sévérité que du JSON, jamais du texte brut, même sur stderr,
  et l'alerte cloud_run_job_failed ne lit que ERROR.

Bibliothèque standard seulement : l'image n'embarque ni gcloud ni client GCS.
"""

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

TARGET = Path("target")
URL_JETON = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
URL_DEPOT = "https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=media&name={nom}"


def lire_artefact(nom):
    chemin = TARGET / nom
    if not chemin.exists():
        return None
    try:
        return json.loads(chemin.read_text())
    except ValueError:
        return None


def journaliser(severite, message, **champs):
    # Même format que journal.py côté ingestion : une ligne, `severity` pilote l'alerting,
    # les autres clés deviennent des champs filtrables (jsonPayload.selection).
    ligne = {"severity": severite, "message": message, "logger": "publish_run", **champs}
    print(json.dumps(ligne, ensure_ascii=False, default=str), flush=True)


def compter_statuts(artefact):
    comptes = {}
    for resultat in (artefact or {}).get("results", []):
        statut = str(resultat.get("status", "")).lower()
        comptes[statut] = comptes.get(statut, 0) + 1
    return comptes


def deposer(uri, contenu):
    if not uri.startswith("gs://"):
        # Chemin local : tests hors Cloud Run.
        chemin = Path(uri)
        chemin.parent.mkdir(parents=True, exist_ok=True)
        chemin.write_bytes(contenu)
        return
    bucket, _, nom = uri.removeprefix("gs://").partition("/")
    # Jeton du compte de service du job, servi par le serveur de métadonnées de Cloud Run.
    demande_jeton = urllib.request.Request(URL_JETON, headers={"Metadata-Flavor": "Google"})
    with urllib.request.urlopen(demande_jeton, timeout=10) as reponse:
        jeton = json.load(reponse)["access_token"]
    envoi = urllib.request.Request(
        URL_DEPOT.format(bucket=bucket, nom=urllib.parse.quote(nom, safe="")),
        data=contenu,
        method="POST",
        headers={"Authorization": f"Bearer {jeton}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(envoi, timeout=30):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--exit-code", type=int, required=True)
    parser.add_argument("--freshness-exit-code", type=lambda v: int(v) if v else None, default=None)
    parser.add_argument("--started-at", required=True)
    args = parser.parse_args()

    fin = datetime.now(timezone.utc)
    commande = os.environ.get("DBT_COMMAND", "build")
    if commande == "snapshot":
        selection = "snapshots"
    elif os.environ.get("DBT_SELECTOR_NAME"):
        selection = f"selector:{os.environ['DBT_SELECTOR_NAME']}"
    else:
        selection = os.environ.get("DBT_TAG_SELECTOR")
    execution = os.environ.get("CLOUD_RUN_EXECUTION") or f"local-{fin:%Y%m%dT%H%M%S}"
    run_results = lire_artefact("run_results.json")
    sources = lire_artefact("sources.json")
    champs = {"execution": execution, "selection": selection}

    # Une seule ligne ERROR par exécution, l'échec dbt en tête : l'alerte n'envoie
    # qu'un mail par tranche de 5 min, un second échec de la même exécution serait perdu.
    problemes = []
    if args.exit_code:
        if (run_results or {}).get("results"):
            c = compter_statuts(run_results)
            detail = (
                f"{c.get('error', 0)} erreur(s), {c.get('fail', 0)} test(s) en échec, "
                f"{c.get('skipped', 0)} sauté(s)"
            )
        else:
            detail = "aucun résultat dbt, échec avant l'exécution des modèles"
        problemes.append(f"[DBT-FAILED] {commande} {selection} : {detail}")
    if args.freshness_exit_code:
        en_erreur = [
            r["unique_id"].split(".", 2)[-1]
            for r in (sources or {}).get("results", [])
            if str(r.get("status", "")).lower() not in ("pass", "warn")
        ]
        detail = ", ".join(en_erreur) or "requête en échec"
        problemes.append(f"[FRESHNESS-ERROR] {os.environ.get('DBT_SOURCE_SELECTOR')} : {detail} (build poursuivi)")
    if problemes:
        journaliser("ERROR", " | ".join(problemes), **champs)

    uri = os.environ.get("DBT_ARTIFACTS_URI")
    if not uri:
        print("[artefacts] DBT_ARTIFACTS_URI absente : exécution non historisée", flush=True)
        return
    enveloppe = {
        "context": {
            "job": os.environ.get("CLOUD_RUN_JOB"),
            "execution": execution,
            "command": commande,
            "selection": selection,
            "freshness_selection": os.environ.get("DBT_SOURCE_SELECTOR") or None,
            "full_refresh": os.environ.get("DBT_FULL_REFRESH") == "true",
            "exit_code": args.exit_code,
            "freshness_exit_code": args.freshness_exit_code,
            "started_at": args.started_at,
            "finished_at": f"{fin:%Y-%m-%dT%H:%M:%SZ}",
        },
        "run_results": run_results,
        "sources": sources,
    }
    # Préfixe dt= : un dossier par jour, prêt pour un hive partitioning si le volume
    # l'exige (bq_ext_dbt_artifacts.tf n'en déclare pas aujourd'hui).
    destination = f"{uri.rstrip('/')}/dt={fin:%Y-%m-%d}/{execution}.json"
    deposer(destination, json.dumps(enveloppe, ensure_ascii=False, separators=(",", ":")).encode())
    print(f"[artefacts] exécution historisée : {destination}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as erreur:
        # Les lignes ERROR sont déjà écrites : seul l'historique manque. Une ligne
        # WARNING plutôt qu'une trace multi-ligne, illisible dans Cloud Logging.
        journaliser("WARNING", f"[artefacts] exécution non historisée : {erreur!r}")
