# Architecture — GAC

| | |
|---|---|
| Source dbt | `gac` (`models/staging/gac/_gac__sources.yml`) |
| Pipeline dlt | `gac` (`ingestion/pipelines/gac`) |
| Tables raw | `prod_raw.gac_sinistre`, `prod_raw.gac_parc_vehicule` |
| Chargement | `gac_sinistre` : `merge` / `delete-insert` sur `snapshot_date` ; `gac_parc_vehicule` : `replace` (fichier unique écrasé) |

Fraîcheur : `docs/freshness.md`. Cadence et orchestration : `docs/pipeline-schedule.md`.

GAC est le prestataire d'assurance de la flotte. Il dépose sur le SFTP EVS deux
types de fichiers CSV (`;`, latin-1) : le suivi des **sinistres** véhicules
(fichier daté, cumulatif) et le **parc véhicule** (fichier statique).

Chaîne :
- `stg_gac__sinistres` → mart `fct_services_generaux__sinistre` (sélection, pas d'enrichissement) ;
- `stg_gac__vehicule` → `int_gac__vehicule_code_analytique`.

Pas de dimension dédiée : véhicule, collaborateur et centre de coûts restent
dénormalisés dans le fait.

---

## Grain et clés

| Modèle | Grain | Clé |
|---|---|---|
| `gac_sinistre` (raw) | 1 sinistre dans 1 snapshot daté | aucune clé de ligne |
| `stg_gac__sinistres` | 1 sinistre (dernière version) | `n_de_sinistre`, sinon `reference_gac` |
| `gac_parc_vehicule` (raw) | 1 période de contrat type (déclaration AEN) | un véhicule peut porter plusieurs lignes |
| `int_gac__vehicule_code_analytique` | 1 immatriculation | `contrat_immatriculation_edi` |

---

## Pièges

### Pas de clé unique sur les sinistres
- **Symptôme** : des sinistres sans `n_de_sinistre`.
- **Règle** : le sinistre est identifié par `n_de_sinistre` ou, à défaut, par
  `reference_gac`. Le staging déduplique sur cette clé conditionnelle en gardant
  le snapshot le plus récent (`snapshot_date`, puis `_extracted_at`). Dans un
  mart ou une jointure, ne jamais utiliser `n_de_sinistre` seul : préférer
  `coalesce(n_de_sinistre, reference_gac)`.
- **Appliqué par** : `stg_gac__sinistres`.

### `collaborateur_fonction_actuelle` contient le code analytique comptable
- **Symptôme** : une colonne qui ressemble à un poste ou une fonction contient
  des codes de type `SAVLYOTECH`.
- **Règle** : malgré son nom, elle porte le **code analytique (compta)** du
  véhicule, pas un libellé de poste. Elle est exposée sous son vrai nom
  `code_analytique`. Les véhicules sans code (pool, non affectés) et les
  immatriculations non réelles (références internes GAC préfixées `#`) sont
  exclus.
- **Appliqué par** : `int_gac__vehicule_code_analytique` (dernière période de
  contrat par immatriculation).

### En-têtes accentués mutilés dans le raw
- **Symptôme** : colonnes brutes `co_t_global`, `cl_turx`, `r_f_rence_gac`,
  `immatx`, `pr_nom`, `centre_de_co_ts`…
- **Règle** : le normaliseur dlt remplace les accents par des underscores (ou un
  `x` final). Le staging renomme proprement. Toujours partir du staging.
- **Appliqué par** : `stg_gac__sinistres`, `stg_gac__vehicule`.

### Dates au format `dd/mm/yyyy [hh:mm:ss]`
- **Symptôme** : une date NULL alors que le CSV la porte.
- **Règle** : le CSV expose toutes les dates en format français, parsées par
  `safe.parse_timestamp` / `safe.parse_date` : une valeur malformée devient NULL
  sans erreur. Surveiller `date_sinistre is null` si le format du fichier dérive.
- **Appliqué par** : `stg_gac__sinistres`.

### Coûts castés sans `safe_cast`
- **Symptôme** : le build échoue sur un cast de coût.
- **Règle** : `cout_assureur`, `auto_assurance`, `franchise`, `cout_global`,
  `cout_client` sont castés en `float64` directement. Un caractère parasite ou
  une virgule décimale française fait échouer le build plutôt que de produire
  un coût faux. Ne pas passer en `safe_cast` sans journaliser les rejets.
- **Appliqué par** : `stg_gac__sinistres`.

---

## Règles métier

- Les statuts de sinistre (`Clos`, `A la route`) et de contrat sont ceux de GAC,
  repris tels quels.
- `resp` est le taux de responsabilité (0 à 100).
- La date métier d'un snapshot (`snapshot_date`) est tirée du nom du fichier
  (`suivi_sinistres_EB_YYYYMMDD_*.csv`), pas de la date de chargement.

---

## Consommateurs

```bash
dbt ls -s source:gac+
```
