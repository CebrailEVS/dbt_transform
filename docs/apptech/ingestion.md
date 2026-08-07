# Ingestion apptech — suivi technique (contexte & pattern)

> Doc de référence de la source `apptech` (app interne de suivi technique).
> Itération 1 (types `pause` + `rw`) : PR #178 + infra `80b6cdd`, appliquée.
> Itération 2 (2026-07-15) : les 6 types suivants (`aguila`, `astreinte`,
> `curative`, `events`, `mee`, `modif_intervention`) — tables externes
> appliquées + staging/tests dans cette PR. **Les 8 types du bucket sont
> couverts.** Pour un futur type : checklist §4.

## 1. Contexte

L'**app interne de suivi technique** (construite par l'analyste, SA `evs-app-tech`)
sert aux managers à saisir des **retraitements sur les interventions techniques**
(overrides de primes, re-mappings d'ids, événements techniciens…). Elle écrit des
NDJSON dans `gs://evs-datastack-apptech/suivi_tech/`.

### Contrat avec l'app (accordé avec l'analyste, 2026-07)

- Layout : `suivi_tech/ingestion/<type>/<YYYY-MM>/<timestampZ>.ndjson`
  (ex. `pause/2026-06/20260710T074944Z.ndjson`).
- Fichiers **immuables** : toute modification = **nouveau fichier horodaté**,
  jamais d'écrasement.
- Chaque fichier = **snapshot COMPLET du (type, mois)** → seul le dernier fichier
  d'un mois fait foi ; une ligne absente du dernier snapshot = retraitement révoqué.
- `suivi_tech/drafts/` = état applicatif interne : **ne jamais le lire**.
- Ajout d'un champ par l'app : annoncé, puis répliqué dans le schéma TF
  (`ignore_unknown_values = true` protège entre-temps).

## 2. Ce qui existe déjà

### Infra (`/mnt/data/infra`, master, appliqué)

- `bigquery_external_tables.tf` : tables externes `prod_raw.ext_gcs_apptech__suivi_tech_pause`
  et `_rw` — NDJSON, **schéma explicite** (pas d'autodetect), `ignore_unknown_values`,
  URI glob `.../ingestion/<type>/*.ndjson` (traverse les dossiers mensuels).
  Naming : `ext_gcs_apptech__suivi_tech_<type>`.
- `app_data_access.tf` : `locals.apptech_bucket_readers` → grant `storage.objectViewer`
  sur le bucket pour `dbt_analysts`, `meltano_runner`, `dbt_dev` (BigQuery lit GCS
  avec les droits du principal requêteur). **Déjà en place — rien à ajouter en IAM
  pour un nouveau type.**

### dbt (ce repo)

- `models/staging/apptech/` : `_apptech__sources.yml`, `_apptech__models.yml`,
  `stg_apptech__suivi_tech_pause.sql`, `stg_apptech__suivi_tech_rw.sql`.
- Tag `apptech` hérité par dossier (`dbt_project.yml`) — un nouveau modèle dans le
  dossier est taggé automatiquement.
- Validé : `dbt build --select tag:apptech --target dev` → 15/15 PASS.

### Le pattern staging (à répliquer pour chaque type)

Voir `stg_apptech__suivi_tech_pause.sql` — structure `source_data` → `cleaned_data`
→ select final avec :

- Métadonnées dérivées de `_file_name` : `periode` (regex sur le dossier
  `YYYY-MM`), `source_file`, `extracted_at`
  (`safe.parse_timestamp('%Y%m%dT%H%M%SZ', …)` sur le nom de fichier).
- **Dedup snapshot** : `qualify extracted_at = max(extracted_at) over (partition by periode)`.
- Nettoyage léger : `nullif(trim(...), '')` sur les champs libres, `upper`/`lower`
  sur les codes. Passthrough des noms de colonnes (convention staging).
- Matérialisation `table` (défaut), pas de partition (volumes minuscules).
- Tests (syntaxe dbt ≥ 1.11 : `arguments:` + severity sous `config:`) :
  `unique_combination_of_columns (periode, <clé>)`, `not_null` sur clé/periode/extracted_at,
  `accepted_values` sur les codes, `expression_is_true` en `warn` pour les
  cohérences de saisie. **Pas de freshness** (soumissions humaines événementielles).

## 3. Les 6 types de l'itération 2 (intégrés — schémas observés au 2026-07-15)

Schémas profilés sur l'ensemble des fichiers du bucket :

| Type | Clé probable | Champs observés |
|---|---|---|
| `aguila` | `intervention_id` | `intervention_id` (str), `convertir_code_5` (OUI/NON), `commentaire` |
| `astreinte` | `intervention_id` | `intervention_id` (str), `tech_id_reel` (str, ex. `tec-00056`), `tech_yuman_id_reel` (**number**), `commentaire` |
| `curative` | `intervention_id` | `intervention_id` (str), `delai_tech_force` (str, ex. `J+0`), `delai_partenaire_force` (str), `commentaire` |
| `events` | `evt_id` (**pas** intervention_id) | `evt_id` (**number**), `evt_tech_yuman_id` (number), `evt_tech_name`, `evt_tech_secteur`, `evt_event_type` (ex. `Conges`), `evt_event_unit` (ex. `Journee`), `evt_event_valeur` (**float**), `evt_event_date` (str `YYYY-MM-DD`), `evt_event_manager_comment`, `mois` (number), `annee` (number) |
| `mee` | `intervention_id` | `intervention_id` (str), `a_facturer` (OUI/NON), `commentaire` |
| `modif_intervention` | `intervention_id` | `intervention_id` (str), `a_facturer` (str, **peut être `""`** → nullif), `tech_id_reel` (str), `tech_yuman_id_reel` (number), `commentaire` |

Points d'attention :

- **Types JSON ≠ CSV** : le schéma de la table externe doit matcher le type JSON
  réel — ids saisis = STRING même si numériques ; `tech_yuman_id_reel`, `evt_id`,
  `mois`, `annee` = INT64 ; `evt_event_valeur` = **FLOAT64** (5.0 dans le JSON).
  Vérifier sur plusieurs lignes avant de figer (échantillonner via
  `gcloud storage cat gs://…/<type>/**/*.ndjson | head`).
- `events` vit sur un **référentiel différent** (événements techniciens, pas
  interventions) : clé `evt_id` — confirmer avec l'analyste si `evt_id` est unique
  globalement ou par mois ; il porte aussi `mois`/`annee` → répliquer le test
  `expression_is_true` de cohérence avec `periode` (cf. rw).
- `pause` a maintenant **2 fichiers sur 2026-06** → le dedup snapshot est exercé
  en réel pour la première fois : après build, vérifier que seules les lignes du
  fichier `20260715T121911Z` sortent du staging.
- Étendre la description de `accepted_values` seulement sur les codes confirmés
  (OUI/NON) ; pour `delai_*_force` demander la liste des valeurs possibles avant
  de contraindre (sinon `not_null` seul).

## 4. Checklist pour ajouter un type

1. **Infra d'abord** (`/mnt/data/infra`, direct sur master — pas de branche) :
   ajouter `resource "google_bigquery_table" "ext_gcs_apptech_suivi_tech_<type>"`
   dans `bigquery_external_tables.tf` (copier le bloc pause, adapter schéma + URI).
   `terraform validate` → `plan` (ne doit montrer QUE les adds attendus) → `apply`.
   Sanity : `bq query 'select count(*), count(distinct _file_name) from prod_raw.ext_gcs_apptech__suivi_tech_<type>'`.
2. **dbt** (branche feature depuis master) : entrée dans `_apptech__sources.yml`,
   `stg_apptech__suivi_tech_<type>.sql` (pattern §2), doc + tests dans
   `_apptech__models.yml`. Rien à toucher dans `dbt_project.yml` (tag hérité).
3. **Valider** : `sqlfluff lint models/staging/apptech/` puis
   `dbt build --select tag:apptech --target dev` (env : `source dbt_venv/bin/activate`
   + `set -a && source .env && set +a`).
4. PR → Slim CI ne build que les nouveaux nœuds → squash merge → CD prod.

Style : guillemets **doubles** pour les strings Jinja contenant des apostrophes
(pas de `\'`) ; conventions staging dans `docs/conventions/staging.md`.

## 5. Aval (hors scope ingestion, à la main de Cebrail)

- Jointures des `intervention_id` vers les référentiels (formats différents selon
  les types : ids courts ~5 chiffres vs longs ~7 chiffres — référentiels distincts
  à confirmer, probablement Yuman workorders vs interventions Nomad/nesp_tech).
- Tests `relationships` en `warn` vers les staging d'interventions une fois les
  référentiels confirmés (filet anti-fautes de saisie).
- Branchement dans les marts (primes, facturation, planning technicien).
