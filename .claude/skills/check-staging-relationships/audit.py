#!/usr/bin/env python
"""
Audite les tests `relationships` d'une couche staging contre le schéma de la source.

Lecture seule : ne modifie aucun `.yml`, n'exécute aucun test, n'écrit rien.

    python audit.py oracle_lcdp
    python audit.py oracle_lcdp --relations ../ingestion/pipelines/oracle_lcdp/relations.json
    python audit.py oracle_lcdp --tout        # inclut les FK dont la cible n'est pas modélisée

Le but n'est PAS d'atteindre une couverture complète : tout n'est pas modélisé en
staging, et une FK dont une extrémité manque n'est pas un oubli. L'univers de l'audit
est donc restreint aux FK dont les DEUX tables ont un modèle staging.

Le rapprochement se fait à la COLONNE : (modèle porteur, colonne testée, modèle cible).
`column_name` est toujours présent dans le manifest, documenté ou non, donc c'est une
clé fiable. Cela suppose que le staging ne renomme pas les colonnes de clé — c'est la
convention du dépôt, et la famille « colonne inattendue » la surveille au lieu de la
supposer.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

MANIFEST = Path("target/manifest.json")


def _charger(source: str, chemin_relations: Path):
    if not MANIFEST.exists():
        sys.exit(f"{MANIFEST} absent : lancer `dbt parse` d'abord")
    manifest = json.loads(MANIFEST.read_text())
    if not chemin_relations.exists():
        sys.exit(
            f"{chemin_relations} absent. Le générer côté ingestion :\n"
            f"  discover.py pipelines/{source} --relations-json pipelines/{source}/relations.json"
        )
    return manifest, json.loads(chemin_relations.read_text())


def _modeles_par_table(manifest, source: str) -> dict[str, str]:
    """table de la source -> nom du modèle staging, via les `source()` déclarés.

    Résolu depuis le manifest et jamais depuis une convention de nommage : le modèle
    `stg_oracle_lcdp__label_company` lit `lcdp_v_label_company`, le préfixe `v_` tombe.
    """
    tables_source = {
        v["unique_id"]: v["name"]
        for v in manifest["sources"].values()
        if v["source_name"] == source
    }
    par_table = {}
    for n in manifest["nodes"].values():
        if n.get("resource_type") != "model":
            continue
        for dep in n["depends_on"]["nodes"]:
            if dep in tables_source:
                par_table[tables_source[dep]] = n["name"]
    return par_table


def _tests_existants(manifest) -> dict[str, list[dict]]:
    """modèle porteur -> ses tests relationships."""
    par_modele = defaultdict(list)
    for n in manifest["nodes"].values():
        meta = n.get("test_metadata") or {}
        if n.get("resource_type") != "test" or meta.get("name") != "relationships":
            continue
        kwargs = meta.get("kwargs") or {}
        cible = re.search(r"ref\(\s*['\"]([^'\"]+)['\"]", str(kwargs.get("to", "")))
        porteur = (n.get("attached_node") or "").split(".")[-1]
        par_modele[porteur].append(
            {
                "colonne": n.get("column_name"),
                "modele_cible": cible.group(1) if cible else None,
                "champ_cible": kwargs.get("field"),
                "severity": (n.get("config") or {}).get("severity", "ERROR"),
                "to_brut": str(kwargs.get("to", "")),
            }
        )
    return par_modele


def _colonnes_reelles(dataset: str, prefixe: str) -> dict[str, set[str]]:
    """modèle -> colonnes RÉELLEMENT exposées, depuis INFORMATION_SCHEMA.

    Indispensable : un modèle staging est une sélection, pas une copie. Sans ce
    contrôle, une FK dont le modèle n'expose même pas la colonne serait annoncée comme
    « test manquant » — un faux positif. Mesuré sur oracle_lcdp : 21 des 43 manquantes
    n'étaient pas testables du tout.
    """
    import csv  # noqa: PLC0415
    import subprocess  # noqa: PLC0415

    requete = (
        f"select table_name, column_name from `{dataset}.INFORMATION_SCHEMA.COLUMNS` "
        f"where table_name like '{prefixe}%'"
    )
    projet = dataset.split(".")[0]
    res = subprocess.run(  # noqa: S603
        ["bq", "query", f"--project_id={projet}", "--use_legacy_sql=false",  # noqa: S607
         "--format=csv", "--max_rows=100000", requete],
        capture_output=True, text=True, check=False,
    )
    if res.returncode != 0:
        sys.exit(f"lecture d'INFORMATION_SCHEMA impossible :\n{res.stderr.strip()[:400]}")
    par_modele: dict[str, set[str]] = {}
    for ligne in csv.DictReader(res.stdout.splitlines()):
        par_modele.setdefault(ligne["table_name"], set()).add(ligne["column_name"])
    return par_modele


def _sql_orphelins(manquants, dataset: str) -> str:
    """Une requête, un UNION ALL par FK manquante : lignes orphelines et total.

    `where enfant is not null` reproduit le comportement du test `relationships` de dbt,
    qui ignore les NULL. Sans ça, toute FK nullable remonterait un orphelin par ligne
    non renseignée et le résultat serait illisible.
    """
    morceaux = []
    for (porteur, colonne, cible), r in manquants:
        tgt = r["colonnes_cible"][0]
        morceaux.append(
            f"select '{porteur}' as modele, '{colonne}' as colonne, '{cible}' as cible,\n"
            f"       countif(c.{tgt} is null) as orphelins, count(*) as lignes_testees\n"
            f"from `{dataset}.{porteur}` s\n"
            f"left join `{dataset}.{cible}` c on s.{colonne} = c.{tgt}\n"
            f"where s.{colonne} is not null"
        )
    return "\nunion all\n".join(morceaux) + "\norder by orphelins desc, modele, colonne"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("source", help="nom de la source dbt (ex. oracle_lcdp)")
    p.add_argument("--relations", type=Path, help="chemin du relations.json du pipeline")
    p.add_argument("--tout", action="store_true", help="lister aussi les FK non modélisables")
    p.add_argument(
        "--dataset",
        metavar="PROJET.DATASET",
        help="dataset des modèles staging (ex. evs-datastack-prod.prod_staging). Permet de "
        "vérifier qu'une colonne de FK est réellement exposée avant de la dire non testée.",
    )
    p.add_argument(
        "--sql",
        metavar="DATASET",
        help="émettre la requête qui compte les orphelins des FK manquantes "
        "(ex. evs-datastack-prod.prod_staging). N'exécute rien.",
    )
    args = p.parse_args()

    relations_path = args.relations or Path(
        f"../ingestion/pipelines/{args.source}/relations.json"
    )
    manifest, contrat = _charger(args.source, relations_path)

    modele_de = _modeles_par_table(manifest, args.source)
    # Le contrat nomme les tables de la SOURCE ; le manifest, les tables BigQuery.
    raw_de_table = {t: d["table_raw"] for t, d in contrat["tables"].items()}
    table_de_raw = {v: k for k, v in raw_de_table.items()}
    modele_par_table = {
        table_de_raw[raw]: modele for raw, modele in modele_de.items() if raw in table_de_raw
    }
    modele_vers_table = {m: t for t, m in modele_par_table.items()}

    tests = _tests_existants(manifest)
    modelisees = set(modele_par_table)

    entete = not args.sql  # avec --sql, stdout ne porte QUE la requête
    if entete:
        print(f"\n{'=' * 78}")
        print(f"AUDIT DES RELATIONSHIPS — source {args.source}")
        print(f"{'=' * 78}")
        print(f"  {len(contrat['tables'])} tables répliquées, {len(modelisees)} modélisées.")
        print(f"  {len(contrat['relations'])} clés étrangères déclarées par la source.")

    # Univers : les deux extrémités modélisées.
    univers, hors = [], []
    for r in contrat["relations"]:
        if r["source"] in modelisees and r["cible"] in modelisees:
            univers.append(r)
        else:
            hors.append(r)
    if entete:
        print(f"  {len(univers)} auditables (deux tables modélisées), {len(hors)} hors univers.\n")

    # Rapprochement à la COLONNE, pas seulement à la paire de tables. `column_name` est
    # toujours présent dans le manifest, documenté ou non : c'est une clé fiable. Cela
    # suppose que le staging ne renomme pas les colonnes de clé — c'est la convention du
    # dépôt, et la famille « colonne inattendue » ci-dessous la surveille.
    attendus = {
        (modele_par_table[r["source"]], r["colonnes"][0], modele_par_table[r["cible"]]): r
        for r in univers
    }

    conformes, colonne_inattendue, a_confirmer = [], [], []
    couverts = set()
    for porteur, liste in sorted(tests.items()):
        if porteur not in modele_vers_table:
            continue  # test posé ailleurs que dans cette couche staging
        for t in liste:
            cle = (porteur, t["colonne"], t["modele_cible"])
            if cle in attendus:
                conformes.append((cle, attendus[cle], t))
                couverts.add(cle)
            elif any(k[0] == porteur and k[2] == t["modele_cible"] for k in attendus):
                colonne_inattendue.append((porteur, t))
            else:
                a_confirmer.append((porteur, t))

    manquants = [(cle, r) for cle, r in sorted(attendus.items()) if cle not in couverts]

    # Un modèle staging SÉLECTIONNE des colonnes : une FK dont la colonne n'est pas
    # exposée n'est pas un test manquant, c'est un choix de modélisation.
    non_exposees = []
    dataset = args.dataset or args.sql
    if dataset:
        reelles = _colonnes_reelles(dataset, f"stg_{args.source}__")
        testables = []
        for cle, r in manquants:
            porteur, colonne, _ = cle
            if colonne in reelles.get(porteur, set()):
                testables.append((cle, r))
            else:
                non_exposees.append((cle, r))
        manquants = testables

    # `--sql` produit UNIQUEMENT la requête, pour pouvoir être redirigé vers `bq`.
    if args.sql:
        if not manquants:
            sys.exit("aucune FK manquante : rien à mesurer")
        print(_sql_orphelins(manquants, args.sql))
        return 0

    print(f"{'-' * 78}\n1. CONFORMES — {len(conformes)} FK déclarées et testées")
    print(f"{'-' * 78}")
    for (porteur, colonne, cible), r, t in conformes:
        champ_attendu = ",".join(r["colonnes_cible"])
        alerte = "" if t["champ_cible"] == champ_attendu else f"   <-- cible {t['champ_cible']}"
        print(f"  {porteur}.{colonne} -> {cible}.{champ_attendu}{alerte}")

    print(f"\n{'-' * 78}\n2. À CONFIRMER — {len(a_confirmer)} tests sans FK correspondante")
    print(f"{'-' * 78}")
    print("  Pas forcément faux : l'ERP n'impose pas tous ses liens logiques. Mais ce")
    print("  test ne repose sur AUCUNE contrainte déclarée — donc sur une hypothèse.")
    print("  À documenter comme connaissance métier, ou à corriger.\n")
    for porteur, t in a_confirmer:
        print(f"  {porteur}.{t['colonne']} -> {t['modele_cible']}.{t['champ_cible']} [{t['severity']}]")

    if colonne_inattendue:
        print(f"\n{'-' * 78}\n2b. COLONNE INATTENDUE — {len(colonne_inattendue)} tests")
        print(f"{'-' * 78}")
        print("  Le lien entre ces deux modèles EST déclaré par la source, mais pas par")
        print("  cette colonne. Trois causes possibles, à distinguer à la main :")
        print("    - la colonne existe et référence bien la cible, sans contrainte Oracle ;")
        print("    - le test est posé sur la mauvaise colonne ;")
        print("    - la colonne a été renommée en staging, contre la convention du dépôt.\n")
        for porteur, t in colonne_inattendue:
            print(f"  {porteur}.{t['colonne']} -> {t['modele_cible']}.{t['champ_cible']}")

    print(f"\n{'-' * 78}\n3. MANQUANTS — {len(manquants)} FK déclarées, exposées, non testées")
    print(f"{'-' * 78}")
    print("  Les deux modèles existent, la source déclare le lien, aucun test ne le")
    print("  couvre. C'est la liste actionnable.\n")
    for (porteur, colonne, cible), r in manquants:
        tgt = ",".join(r["colonnes_cible"])
        print(f"  {porteur}.{colonne:30} -> {cible}.{tgt}")
    if non_exposees:
        print(f"\n{'-' * 78}\n3b. COLONNE NON EXPOSÉE — {len(non_exposees)} FK\n{'-' * 78}")
        print("  La source déclare le lien, mais le modèle staging ne sélectionne pas la")
        print("  colonne. Ce n'est PAS un test manquant : c'est un choix de modélisation.")
        print("  Pour tester, il faudrait d'abord exposer la colonne.\n")
        for (porteur, colonne, cible), _ in non_exposees:
            print(f"  {porteur}.{colonne:32} (cible {cible})")
    elif not dataset:
        print("\n  Sans --dataset, la liste ci-dessus inclut peut-être des colonnes que le")
        print("  modèle staging n'expose pas : elles ne sont alors PAS testables.")

    if manquants:
        print("\n  AVANT d'ajouter : mesurer les orphelins. Aucun `severity` n'est déclaré")
        print("  dans ce projet, donc tout test est en ERROR et casserait le build nocturne")
        print("  si le lien est légitimement violé (merge sans suppression, purge")
        print("  hebdomadaire, extraction non atomique).")
        print("  `--sql <projet.dataset>` produit la requête qui les compte toutes.")
        (porteur, colonne, cible), r = manquants[0]
        tgt = ",".join(r["colonnes_cible"])
        print()
        print(f"    select count(*) from {{{{ ref('{porteur}') }}}} s")
        print(f"    left join {{{{ ref('{cible}') }}}} c on s.{colonne} = c.{tgt}")
        print(f"    where s.{colonne} is not null and c.{tgt} is null")

    if args.tout and hors:
        print(f"\n{'-' * 78}\n4. HORS UNIVERS — {len(hors)} FK non auditables\n{'-' * 78}")
        for r in hors:
            absente = r["source"] if r["source"] not in modelisees else r["cible"]
            print(f"  {r['source']} -> {r['cible']:30} ({absente} non modélisée en staging)")
    elif hors:
        print(f"\n  ({len(hors)} FK hors univers, une extrémité non modélisée — `--tout` pour la liste)")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
