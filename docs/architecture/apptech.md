# Architecture — apptech (app Suivi Tech)

> **Aucun cron.** La chaîne `apptech` n'est branchée sur aucun pipeline planifié, par choix :
> son build doit être déclenché par l'application Suivi Tech elle-même, au moment de la saisie.
> **Ne jamais l'ajouter au `DBT_TAG_SELECTOR` d'un workflow** : le jour où l'app déclenche, la
> chaîne serait construite deux fois. Dérogation déclarée dans `infra/CLAUDE.md` (charte
> d'orchestration, frontière dbt).

| | |
|---|---|
| Source dbt | `apptech` (`models/staging/apptech/_apptech__sources.yml`) |
| Producteur | application interne Suivi Tech, maintenue par le data analyst : fichiers NDJSON dans le bucket apptech (`infra/app_data_access.tf`) |
| Chargement | tables externes `prod_raw.ext_gcs_apptech__suivi_tech_<type>` (`infra/bq_ext_apptech.tf`) : elles lisent tous les fichiers, le staging ne garde que le dernier snapshot de chaque mois |

Fraîcheur : aucune, volontairement : saisies humaines sans cadence ([`docs/freshness.md`](../freshness.md)).

Les managers y saisissent des **retraitements d'interventions techniques** (primes, délais,
facturation, technicien réel) et des événements techniciens.

## 1. Contrat avec l'application

- Chemin : `suivi_tech/ingestion/<type>/<AAAA-MM>/<AAAAMMJJ>T<hhmmss>Z.ndjson`.
- Fichiers **immuables** : toute modification produit un **nouveau fichier horodaté**, jamais un
  écrasement. Les fichiers ne sont jamais supprimés : les tables externes lisent tout l'historique.
- Chaque fichier est un **snapshot complet du couple (type, mois)** : seul le dernier fichier d'un
  mois fait foi, et une ligne absente du dernier snapshot est un retraitement **révoqué**.
- `suivi_tech/drafts/` est l'état interne de l'application : **ne jamais le lire**.
- Un champ ajouté par l'app est annoncé, puis ajouté au schéma Terraform ;
  `ignore_unknown_values = true` évite la casse entre-temps.

## 2. Types et identité

Un type = un dossier, une table externe, un staging `stg_apptech__suivi_tech_<type>` (liste dans
`_apptech__sources.yml`). Chaque staging est unique sur `(periode, <clé>)` : `intervention_id`
pour les retraitements, `bad_intervention_id` pour `rw` (réaffectation d'une intervention erronée
vers la bonne), `evt_id` pour `events`, qui relève d'un **autre référentiel** que les interventions.

- **Identité** : `key_inter = src_inter || '_' || intervention_id`, la PK de
  `fct_technique__intervention`. `intervention_id` vaut `workorder_id` côté `YUMAN` et
  `n_planning` côté `NESP` ; les deux plages se chevauchent, d'où le discriminant `src_inter`.
  `src_inter` vient du NDJSON ; `int_apptech__retraitements` reconstruit `key_inter`. `rw` ne
  porte pas `src_inter` : ses interventions sont NESP (`fct_technique__credit_repair_warranty`).
- **`numero_pu`** : le numéro d'intervention affiché (« n° PU ») que cherchent les managers,
  `workorder_number` côté `YUMAN`, `n_planning` côté `NESP`. Non unique côté `YUMAN` : affichage et
  recherche seulement, jamais clé de jointure.
- **Agence de facturation = `agency`**, jamais `tech_secteur` : une agence recouvre plusieurs
  secteurs. `agency` n'existe que côté `NESP` (NULL côté `YUMAN`).

## 3. Pièges

**Types JSON, pas CSV.** Une table externe NDJSON exige que le schéma Terraform corresponde au
type réel du JSON émis. Un identifiant saisi est émis en chaîne, donc `STRING` même s'il est
numérique ; un champ émis en nombre est `INT64` ou `FLOAT64` (`5.0` dans le JSON donne `FLOAT64`).
Échantillonner plusieurs fichiers avant de figer un schéma
(`gcloud storage cat gs://<bucket apptech>/suivi_tech/ingestion/<type>/**/*.ndjson | head`).

**Chaîne vide.** Un champ texte peut arriver à `""` (ex. `a_facturer` de `modif_intervention`) :
`nullif(trim(...), '')` en staging.

**`events` et `rw` portent leur propre mois.** Ils ont `mois` et `annee` en plus du dossier
mensuel : un test `expression_is_true` (`warn`) vérifie leur cohérence avec `periode`.

**Codes non confirmés.** Un `accepted_values` ne se pose que sur une liste confirmée par le data
analyst ; sinon `not_null` seul.

## 4. Pattern staging, à répliquer pour chaque type

Modèle de référence : `stg_apptech__suivi_tech_pause.sql`, structure `source_data` →
`cleaned_data` → `select` final.

- Métadonnées tirées du chemin (`_file_name`) : `periode` (dossier `AAAA-MM`), `source_file`,
  `extracted_at` (`safe.parse_timestamp('%Y%m%dT%H%M%SZ', …)` sur le nom du fichier).
- **Dernier snapshot du mois** : `qualify extracted_at = max(extracted_at) over (partition by periode)`.
- Nettoyage léger : `nullif(trim(...), '')` sur les champs libres, `upper` / `lower` sur les
  codes ; noms de colonnes passthrough. Matérialisation `table`, sans partition.
- Tests (paramètres sous `arguments:`, sévérité sous `config:`) :
  `unique_combination_of_columns (periode, <clé>)`, `not_null` sur clé, `periode`,
  `extracted_at` ; `accepted_values` sur les codes confirmés ; `expression_is_true` en `warn`
  pour les cohérences de saisie. Pas de freshness.
- Le tag `apptech` est hérité du dossier (`dbt_project.yml`) : rien à déclarer.
- Guillemets doubles pour une chaîne Jinja qui contient une apostrophe (pas de `\'`).

Conventions de la couche : [`staging.md`](../conventions/staging.md).

## 5. Ajouter un type

1. **Infra d'abord** (dépôt `infra`, commit direct sur `master`) : dans `bq_ext_apptech.tf`,
   copier le bloc `pause`, adapter schéma et URI. `terraform validate`, `plan` (seuls les ajouts
   attendus), `apply`. Contrôle :
   `select count(*), count(distinct _file_name) from prod_raw.ext_gcs_apptech__suivi_tech_<type>`.
   Les droits de lecture du bucket couvrent déjà les identités dbt.
2. **dbt** (branche de feature) : source dans `_apptech__sources.yml`, modèle selon le § 4,
   description et tests dans `_apptech__models.yml`. `dbt lint models/staging/apptech/`, puis
   `dbt build --select tag:apptech` en dev.
3. **PR** : `pr-check` construit les nouveaux modèles en CI ; au merge, `cd` les construit **une
   fois** en prod (`state:modified+`). Ensuite, seul le déclenchement par l'application les
   rafraîchit (voir l'encadré en tête).

## 6. Aval

- **Hors lignage dbt** : l'application lit aussi BigQuery (datasets autorisés dans
  `infra/app_data_access.tf`), sans `ref()`. Avant de renommer ou de retirer une colonne dans ces
  datasets, vérifier avec le data analyst.
- **Consommateurs** : `dbt ls -s source:apptech+ --resource-type model`.
