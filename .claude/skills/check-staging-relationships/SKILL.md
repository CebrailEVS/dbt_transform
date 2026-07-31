---
name: check-staging-relationships
description: Audite les tests relationships d'une couche staging contre les clés étrangères déclarées par la source. Dit lesquels sont conformes, lesquels reposent sur une hypothèse, et lesquels manquent. Lecture seule, ne modifie aucun .yml.
argument-hint: "<source dbt> [--tout] (ex. oracle_lcdp)"
user-invocable: true
---

# Auditer les relationships d'une couche staging

Répond à une seule question : **mes tests `relationships` sont-ils alignés sur
l'architecture de la source ?** Elle propose, elle n'écrit jamais — ni dans les `.yml`,
ni dans l'entrepôt, et elle n'exécute aucun test.

Ce n'est **pas** un objectif de couverture. Tout n'est pas modélisé en staging, et une
clé étrangère dont une extrémité n'a pas de modèle n'est pas un oubli. L'audit se
restreint donc aux FK dont les **deux** tables ont un modèle staging ; le reste sort du
rapport, réduit à un décompte.

## Prérequis

```bash
dbt parse                                    # produit target/manifest.json
```

Et le contrat de la source, généré côté `ingestion/` — ce dépôt ne peut pas interroger
Oracle, et n'a pas à pouvoir le faire :

```bash
cd ../ingestion
P=pipelines/oracle_lcdp
$P/.venv/bin/python .claude/skills/discover-sql-source/discover.py $P \
  --relations-json $P/relations.json
```

Ce fichier ne bouge qu'à une montée de version de l'ERP. Le rafraîchir de temps en
temps, pas à chaque build.

## Arguments

`$ARGUMENTS` — le nom de la source dbt, plus les options.

| Option | Effet |
|---|---|
| *(rien)* | les trois familles, et un décompte des FK hors univers |
| `--tout` | liste aussi les FK hors univers, avec l'extrémité qui manque |
| `--relations <chemin>` | contrat ailleurs que `../ingestion/pipelines/<source>/relations.json` |

```bash
python3 .claude/skills/check-staging-relationships/audit.py oracle_lcdp
```

## Comment lire les trois familles

**1. Conformes.** La source déclare le lien sur cette colonne exacte, un test le couvre.
Le décompte est par FK, pas par paire de tables : sur un ERP plusieurs colonnes relient
souvent les deux mêmes tables (`device` pointe vers `company` par `idcompany_customer`,
`idcompany_owner`, `idcompany_supplier`, `idfinancial`), et tester l'une ne dit rien des
autres. Celles qui manquent apparaissent donc en famille 3, une par une.

**2. À confirmer.** Un test existe, aucune contrainte déclarée ne le fonde. **Ce n'est
pas une erreur** : un ERP n'impose pas tous ses liens logiques, et une colonne
manifestement référentielle peut n'avoir aucune FK. Mais le test repose alors sur une
hypothèse, pas sur un contrat — donc soit on la documente comme connaissance métier,
soit elle est fausse. Le distinguer demande de connaître le métier.

**2b. Colonne inattendue.** Le lien entre les deux modèles est déclaré, mais pas par la
colonne testée. Voir les trois causes ci-dessus.

**3. Manquants.** Les deux modèles existent, la source déclare le lien sur cette colonne,
aucun test ne le couvre. **C'est la liste actionnable.**

## Avant d'ajouter un test manquant : mesurer

Une FK vraie à la source peut être **légitimement fausse** dans l'entrepôt, pour trois
raisons structurelles :

- le `merge` ne supprime jamais — un parent effacé disparaît du référentiel snapshoté
  mais l'enfant survit, jusqu'au `--purge-deletes` hebdomadaire ;
- l'extraction n'est pas atomique entre tables — un parent créé entre deux ressources ;
- le staging filtre des lignes.

Or **aucun `severity` n'est déclaré dans ce projet** : tout test est en `ERROR`. Un test
ajouté sans mesure peut donc casser le build nocturne sur une donnée correcte. Compter
les orphelins d'abord, puis choisir : zéro orphelin → `error` ; quelques orphelins
expliqués → `severity: warn` avec un `error_if` chiffré ; beaucoup → comprendre avant de
tester.

## Ce que la compétence ne sait pas faire

- **Elle suppose que le staging ne renomme pas les colonnes de clé.** C'est la
  convention du dépôt, et la famille 2b la surveille au lieu de la supposer : un test
  dont la colonne ne correspond à aucune FK vers ce modèle y remonte. Trois causes à
  distinguer à la main — colonne référentielle sans contrainte Oracle, test sur la
  mauvaise colonne, ou renommage à corriger.
- **Elle ne mesure pas les orphelins.** Elle fournit la forme de la requête ; la mesure
  se fait avec `dbt` ou `bq`.
- **Elle ne juge pas la famille 2.** Distinguer une connaissance métier légitime d'une
  hypothèse fausse demande de connaître la source.
- **Elle ignore les couches intermediate et marts.** Les FK d'une source décrivent le
  modèle source ; plus haut, les liens sont ceux que la modélisation crée.
