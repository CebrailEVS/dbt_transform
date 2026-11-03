# Architecture — Nespresso Technique (`nesp_tech`)

| | |
|---|---|
| Source dbt | `nesp_tech` — `models/staging/nesp_tech/_nesp_tech__sources.yml` |
| Extraction | **pas de pipeline dlt** : job Cloud Run (`infra/cloudrun_elt.tf`, code hors du dépôt `ingestion`) qui télécharge les rapports Excel du fournisseur et dépose un CSV par onglet dans Cloud Storage ; tables raw externes déclarées dans `infra/bq_ext_nespresso.tf` |
| Chargement | **accumulation de fichiers** : chaque run dépose de nouveaux CSV, jamais supprimés, et la table externe les lit tous. Les rapports se chevauchent (par agence sur les derniers jours, plus le mois en cours toutes agences) : une même intervention apparaît dans plusieurs fichiers, à des états successifs. Le dédoublonnage est entièrement à la charge de dbt |

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).

Le périmètre : interventions de maintenance, réparation et mise en service de machines
Nespresso professionnelles chez les clients, et articles (pièces, consommables) posés pendant
ces interventions. Les deux tables se lient par `n_planning`. Pas de référentiel technicien,
machine ou client : les libellés sont recopiés sur chaque ligne.

---

## Clés et grain

- Le staging garde les **états successifs** d'une intervention (une version par
  `(n_planning, etat_intervention, date_heure_fin)`) ; `int_nesp_tech__interventions_dedup` n'en
  garde qu'un par `n_planning`, par `date_heure_fin` puis `extracted_at` décroissants.
- `int_nesp_tech__articles_dedup` : une ligne par `(n_planning, code_article)`.
- **En aval, partir des intermediates `*_dedup`, jamais du staging.**

---

## Pièges

**Périmètre : les agences EVS, pas le sous-traitant.** Les rapports couvrent aussi un
sous-traitant de Nespresso, qui n'est pas une agence EVS. Règle :
`int_nesp_tech__interventions_dedup` ne garde que les agences EVS (liste dans le modèle), en
point unique ; les filtres d'agence répétés en aval sont redondants. Le staging, lui, contient
tout. Les articles ne portent pas d'agence et `int_nesp_tech__articles_dedup` n'est **pas**
filtré : restreindre un calcul sur les articles au périmètre EVS en le joignant à
`int_nesp_tech__interventions_dedup`.

**Colonnes lues par position.** La table externe a un schéma figé dans Terraform
(`autodetect = false`, en-tête sauté) : les colonnes du CSV sont lues dans l'ordre, pas par
nom. Si le fournisseur ajoute ou déplace une colonne dans son rapport, les valeurs se décalent
sans erreur. Une évolution du rapport se traite d'abord dans `infra/bq_ext_nespresso.tf`.

**Tout est `STRING` au raw, avec des sentinelles pandas.** Le job écrit ses CSV depuis un
DataFrame pandas : valeur absente = `'nan'`, horodatage absent = `'nat'`, date par défaut =
`'01/01/0001'`. Règle : chaque colonne du staging les convertit en `NULL` avant cast. Ne
jamais lire le raw directement.

**Libellés en minuscules.** Le staging applique `lower(trim(...))` à tous les textes (statut,
agence, type, ville…). Tout filtre en aval s'écrit en minuscules.

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

**Délais et facturation ne retiennent pas les mêmes statuts.** Chaque modèle déclare les
états d'intervention qu'il garde : `int_nesp_tech__delais_interventions` et
`int_nesp_tech__facturation_interventions` diffèrent. Lire le filtre du modèle, ne pas le
supposer.

**Le tarif dépend de la date de fin de l'intervention.** La grille est versionnée dans un seed
(`valid_from`, `valid_to`) et `int_nesp_tech__facturation_interventions` applique le tarif en
vigueur à la date de fin en heure de Paris, pas le tarif courant : un mois passé se recalcule à
l'identique. Clé de facturation et grille : seeds `ref_nesp_tech__*`.

---

## Hors lignage dbt

Le job d'export du stock vers Yuman (`infra/cloudrun_export.tf`) lit
`prod_staging.stg_nesp_tech__articles` et `prod_staging.stg_nesp_tech__interventions`. Renommer
ou changer une colonne de ces staging le casse sans qu'aucun test dbt ne le voie.

---

## Consommateurs

```bash
dbt ls -s source:nesp_tech+ --resource-type model
```
