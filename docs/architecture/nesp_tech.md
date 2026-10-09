# Architecture — Nespresso Technique (`nesp_tech`)

| | |
|---|---|
| Source dbt | `nesp_tech` — `models/staging/nesp_tech/_nesp_tech__sources.yml` |
| Extraction | **pas de pipeline dlt** : job Cloud Run `ingest-nesp-tech` (code hors du dépôt `ingestion`, `extract_load/ingest_nesp_tech`) qui télécharge les rapports Excel de Nomad Repair (API Arbiter) et dépose un CSV par onglet dans `gs://evs-datastack-raw-data/nespresso/technique/{interventions,articles}/` |
| Tables raw | `prod_raw.nespresso_technique_interventions`, `prod_raw.nespresso_technique_articles` : tables externes BigQuery sur ces CSV, déclarées dans `infra/bq_ext_nespresso.tf` |
| Fraîcheur | [`docs/freshness.md`](../freshness.md) |
| Cadence | [`docs/pipeline-schedule.md`](../pipeline-schedule.md) ; le workflow enchaîne l'extraction et `dbt build -s source:nesp_tech+` |

**Mode de chargement : accumulation de fichiers.** Chaque run dépose de nouveaux CSV, jamais
supprimés. Les rapports se chevauchent (un rapport par agence sur les derniers jours, plus un
rapport du mois en cours toutes agences) : une même intervention apparaît dans plusieurs
fichiers, à des états successifs. La table externe lit tous les fichiers ; le dédoublonnage
est entièrement à la charge de dbt.

Le périmètre : interventions de maintenance, réparation et mise en service de machines
Nespresso professionnelles chez les clients, et articles (pièces, consommables) posés pendant
ces interventions. Les deux tables se lient par `n_planning`. Pas de référentiel technicien,
machine ou client : les libellés sont recopiés sur chaque ligne.

---

## Grain et clés

| Modèle | Grain | Clé de dédoublonnage |
|---|---|---|
| `stg_nesp_tech__interventions` | 1 état d'intervention | `(n_planning, etat_intervention, date_heure_fin)`, version la plus récente par `extracted_at` |
| `stg_nesp_tech__articles` | 1 article × intervention × date | `(n_planning, code_article, date_intervention)`, version la plus récente par `extracted_at` |
| `int_nesp_tech__interventions_dedup` | 1 intervention, dernier état connu, **périmètre EVS** | `n_planning`, par `date_heure_fin` puis `extracted_at` décroissants |
| `int_nesp_tech__articles_dedup` | 1 article × intervention | `(n_planning, code_article)`, par `extracted_at` décroissant |

Le staging garde les états successifs d'une intervention ; les intermediates `*_dedup` n'en
gardent qu'un. **En aval, partir des intermediates `*_dedup`, jamais du staging.**

---

## Pièges

**Périmètre : les quatre agences EVS, pas le sous-traitant.** Les rapports couvrent aussi
`nespresso sud`, un sous-traitant de Nespresso et non une agence EVS. Règle :
`int_nesp_tech__interventions_dedup` ne garde que `agency in ('evs', 'evs idf', 'evs paris',
'evs paris 2')`, en point unique ; les filtres d'agence répétés en aval sont redondants. Le
staging, lui, contient tout. Les articles ne portent pas d'agence et
`int_nesp_tech__articles_dedup` n'est **pas** filtré : restreindre un calcul sur les articles
au périmètre EVS en le joignant à `int_nesp_tech__interventions_dedup`.

**Colonnes lues par position.** La table externe a un schéma figé dans Terraform
(`autodetect = false`, en-tête sauté) : les colonnes du CSV sont lues dans l'ordre, pas par
nom. Si Nomad Repair ajoute ou déplace une colonne dans son rapport, les valeurs se décalent
sans erreur. Une évolution du rapport se traite d'abord dans `infra/bq_ext_nespresso.tf`.

**Tout est `STRING` au raw, avec des sentinelles pandas.** Le job écrit ses CSV depuis un
DataFrame pandas : valeur absente = `'nan'`, horodatage absent = `'nat'`, date par défaut =
`'01/01/0001'`. Règle : chaque colonne du staging les convertit en `NULL` avant cast. Ne
jamais lire le raw directement.

**Libellés en minuscules.** Le staging applique `lower(trim(...))` à tous les textes (statut,
agence, type, ville…). Tout filtre en aval s'écrit en minuscules : `'terminée signée'`,
`'evs idf'`.

**Artefacts numériques de pandas.** Un code postal stocké en nombre perd son zéro initial, un
code numérique gagne un `.0`. Règle : le staging repréfixe `0` aux codes postaux à 4 chiffres
et retire le `.0` des codes réparation.

**`pickup_date` en deux formats.** ISO, ou `jj/mm/aaaa hh:mm` avec parfois un siècle `00xx`
au lieu de `20xx`. Règle : `safe_cast` puis `parse_timestamp` après correction du siècle ; une
valeur non reconnue devient `NULL`.

**`extracted_at` en formats variables.** Selon le fichier, avec ou sans fraction de seconde,
séparateur `T` ou espace. Règle : `coalesce(safe_cast(...), safe.parse_timestamp('%Y-%m-%d
%H:%M:%E*S%Ez', ...))`. Un `extracted_at` non reconnu fausse le choix de la version la plus
récente.

**Un seul exemplaire d'un article par intervention.** `int_nesp_tech__articles_dedup` garde
une ligne par `(n_planning, code_article)` : deux lignes du même article sur une même
intervention se réduisent à la plus récente, pas à leur somme.

---

## Règles métier et leur source

| Règle | Source | Appliquée dans |
|---|---|---|
| Délais : statuts `terminée signée` et `signature différée` ; départ = `pickup_date`, ou `creation_date` si la prise en charge lui est antérieure ; jours et heures ouvrés jusqu'au début et à la fin, hors week-ends et jours fériés | seed `ref_general__feries_metropole` | `int_nesp_tech__delais_interventions` |
| Facturation : statuts `terminée signée`, `signature différée` **et** `mise en échec` (intervention non aboutie, facturée au forfait) | grille de facturation Nespresso | `int_nesp_tech__facturation_interventions` |
| Clé de facturation : type d'intervention (code réparation, code panne), machine normalisée, présence d'une mini-prév (article `miniprev`), zone montagne (département du code postal du site) | seeds `ref_nesp_tech__key_type_inter`, `ref_nesp_tech__machines_clean`, `ref_nesp_tech__dpts_montagne_factu` | `int_nesp_tech__facturation_interventions` |
| Tarif : celui en vigueur à la date de fin de l'intervention (heure de Paris), pas le tarif courant ; la grille est versionnée (`valid_from`, `valid_to`) pour pouvoir recalculer un mois passé | seed `ref_nesp_tech__key_facturation` (grille de facturation Nespresso) | `int_nesp_tech__facturation_interventions` |

---

## Consommateurs

Les marts `models/marts/technique/` et `fct_commerce__machine_intervention`.
`fct_technique__intervention` réunit les interventions Nespresso et Yuman (`src_inter`), et
est partitionné sur `date_debut`. `fct_commerce__machine_intervention` lit
`int_nesp_tech__interventions_dedup` et lui joint le client (`dim_commerce__client`), sans
opportunité commerciale. Liste à jour :

```bash
dbt ls -s source:nesp_tech+ --resource-type model
```

**Hors lignage dbt** : le job d'export `export-nesp-tech-stock-yuman` (`infra/cloudrun_export.tf`)
lit `prod_staging.stg_nesp_tech__articles` pour pousser les mouvements de stock vers Yuman.
Renommer ou changer une colonne de ce staging le casse sans qu'aucun test dbt ne le voie.
