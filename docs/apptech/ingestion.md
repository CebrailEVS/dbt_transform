# Ingestion apptech — retraitements de l'app Suivi Tech

> **Aucun cron.** La chaîne `apptech` n'est branchée sur aucun pipeline planifié, par choix :
> son build doit être déclenché par l'application Suivi Tech elle-même, au moment de la
> saisie. **Ne jamais l'ajouter au `DBT_TAG_SELECTOR` d'un workflow** : le jour où l'app
> déclenche, la chaîne serait construite deux fois. Tant que l'app ne déclenche pas, les
> modèles restent figés à leur dernier build. Dérogation déclarée dans `infra/CLAUDE.md`
> (charte d'orchestration, frontière dbt).

| | |
|---|---|
| Source dbt | `apptech` — `models/staging/apptech/_apptech__sources.yml` |
| Producteur | application interne Suivi Tech (compte de service `evs-app-tech`), maintenue par le data analyst |
| Fichiers | NDJSON dans `gs://evs-datastack-apptech/suivi_tech/ingestion/<type>/<AAAA-MM>/<horodatageZ>.ndjson` |
| Tables raw | `prod_raw.ext_gcs_apptech__suivi_tech_<type>` : tables externes BigQuery, déclarées dans `infra/bq_ext_apptech.tf` |
| Fraîcheur | aucune, volontairement : saisies humaines sans cadence ([`docs/freshness.md`](../freshness.md)) |
| État du chantier | [`docs/apptech/etat_du_chantier.md`](etat_du_chantier.md), fichier partagé avec le data analyst |

Les managers y saisissent des **retraitements d'interventions techniques** (primes, délais,
facturation, technicien réel) et des événements techniciens.

## 1. Contrat avec l'application

- Chemin : `suivi_tech/ingestion/<type>/<AAAA-MM>/<horodatageZ>.ndjson`
  (ex. `pause/2026-06/20260710T074944Z.ndjson`).
- Fichiers **immuables** : toute modification produit un **nouveau fichier horodaté**, jamais
  un écrasement. Les fichiers ne sont jamais supprimés : les tables externes lisent tout
  l'historique.
- Chaque fichier est un **snapshot complet du couple (type, mois)** : seul le dernier fichier
  d'un mois fait foi, et une ligne absente du dernier snapshot est un retraitement **révoqué**.
- `suivi_tech/drafts/` est l'état interne de l'application : **ne jamais le lire**.
- Un champ ajouté par l'app est annoncé, puis ajouté au schéma Terraform ;
  `ignore_unknown_values = true` évite la casse entre-temps.

## 2. Les huit types

| Type | Contenu | Clé (avec `periode`) |
|---|---|---|
| `pause` | prime « pause » à doubler ou non | `intervention_id` |
| `aguila` | conversion ou non du code 5 Aguila | `intervention_id` |
| `astreinte` | technicien réel différent du planifié | `intervention_id` |
| `curative` | délais technicien et partenaire forcés (`J+0`, `J+2`…) | `intervention_id` |
| `mee` | décision de facturation d'une mise en exploitation (`a_facturer`) | `intervention_id` |
| `modif_intervention` | décision de facturation et/ou technicien réel corrigés | `intervention_id` |
| `rw` | réaffectation d'une intervention erronée vers la bonne | `bad_intervention_id` |
| `events` | événements techniciens (congés, maladie, astreinte…) : **autre référentiel** que les interventions | `evt_id` |

Identité d'une intervention (`src_inter`, `intervention_id`, `numero_pu`) et règles de
jointure vers Yuman et Nomad Repair : [`etat_du_chantier.md` § 4](etat_du_chantier.md#4-décisions-gelées).

## 3. Pièges

**Types JSON, pas CSV.** Une table externe NDJSON exige que le schéma Terraform corresponde
au type réel du JSON émis. Les identifiants saisis sont des `STRING` même quand ils sont
numériques ; les champs émis en nombre (`tech_yuman_id_reel`, `evt_id`, `mois`, `annee`) sont
`INT64`, et `evt_event_valeur` est `FLOAT64` (`5.0` dans le JSON). Échantillonner plusieurs
fichiers avant de figer un schéma
(`gcloud storage cat gs://evs-datastack-apptech/suivi_tech/ingestion/<type>/**/*.ndjson | head`).

**Chaîne vide.** Un champ texte peut arriver à `""` (ex. `a_facturer` de
`modif_intervention`) : `nullif(trim(...), '')` en staging.

**`events` a son propre mois.** Il porte `mois` et `annee` en plus du dossier mensuel : un
test `expression_is_true` vérifie leur cohérence avec `periode`.

**Codes non confirmés.** Un `accepted_values` ne se pose que sur une liste confirmée par le data
analyst (`OUI` / `NON`) ; sinon `not_null` seul.

## 4. Pattern staging, à répliquer pour chaque type

Modèle de référence : `stg_apptech__suivi_tech_pause.sql`, structure `source_data` →
`cleaned_data` → `select` final.

- Métadonnées tirées du chemin (`_file_name`) : `periode` (dossier `AAAA-MM`), `source_file`,
  `extracted_at` (`safe.parse_timestamp('%Y%m%dT%H%M%SZ', …)` sur le nom du fichier).
- **Dernier snapshot du mois** : `qualify extracted_at = max(extracted_at) over (partition by periode)`.
- Nettoyage léger : `nullif(trim(...), '')` sur les champs libres, `upper` / `lower` sur les
  codes ; noms de colonnes passthrough.
- Matérialisation `table`, sans partition.
- Tests (paramètres sous `arguments:`, sévérité sous `config:`) :
  `unique_combination_of_columns (periode, <clé>)`, `not_null` sur clé, `periode`,
  `extracted_at` ; `accepted_values` sur les codes confirmés ; `expression_is_true` en `warn`
  pour les cohérences de saisie. Pas de freshness.
- Le tag `apptech` est hérité du dossier (`dbt_project.yml`) : rien à déclarer.

## 5. Ajouter un type

1. **Infra d'abord** (dépôt `infra`, commit direct sur `master`) : ajouter
   `resource "google_bigquery_table" "ext_gcs_apptech_suivi_tech_<type>"` dans
   `bq_ext_apptech.tf` (copier le bloc `pause`, adapter schéma et URI). `terraform validate`,
   puis `plan` (seuls les ajouts attendus), puis `apply`. Contrôle :
   `select count(*), count(distinct _file_name) from prod_raw.ext_gcs_apptech__suivi_tech_<type>`.
   Les droits de lecture du bucket sont déjà en place pour toutes les identités dbt : rien à
   ajouter en IAM.
2. **dbt** (branche de feature depuis `master`) : entrée dans `_apptech__sources.yml`, modèle
   `stg_apptech__suivi_tech_<type>.sql` (pattern § 4), description et tests dans
   `_apptech__models.yml`.
3. **Valider** : `dbt lint models/staging/apptech/`, puis `dbt build --select tag:apptech`
   en dev (`.env` chargé).
4. **PR** : `pr-check` construit les nouveaux modèles dans `dbt_ci_pr_<N>` ; au merge, `cd`
   les construit **une fois** en prod (`state:modified+`). Ce build initial ne les rafraîchit
   pas ensuite : seul le déclenchement par l'application le fait (voir l'encadré en tête).

Style : guillemets doubles pour une chaîne Jinja qui contient une apostrophe (pas de `\'`).
Conventions de la couche : [`docs/conventions/staging.md`](../conventions/staging.md).

## 6. Aval

Le branchement des retraitements dans les marts technique (facturation retraitée, primes,
avoirs) et les règles qui le gouvernent sont suivis dans
[`etat_du_chantier.md`](etat_du_chantier.md) : décisions au § 4, travaux en cours au § 3.
Liste à jour des modèles en aval : `dbt ls -s source:apptech+ --resource-type model`.
