# Architecture — GAC

| | |
|---|---|
| Source dbt | `gac` (`models/staging/gac/_gac__sources.yml`) |
| Pipeline | `ingestion/pipelines/gac` : CSV déposés par GAC sur le SFTP EVS |
| Chargement | sinistres : `merge` / `delete-insert` sur `snapshot_date`, les snapshots s'accumulent et le staging déduplique ; parc véhicule : `replace`, le fichier écrasé est l'état courant, sans historique |

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).

GAC est le loueur de la flotte de véhicules. Il dépose deux CSV (`;`, latin-1) : le suivi des
**sinistres** (un fichier daté, cumulatif) et le **parc véhicule** (un fichier unique). Pas de
dimension dédiée : véhicule, collaborateur et centre de coûts restent dénormalisés dans le fait
des sinistres.

---

## Clés et grain

- **Sinistres (raw)** : 1 sinistre dans 1 snapshot daté, sans clé de ligne. La date du snapshot
  (`snapshot_date`) vient de la date que GAC écrit dans le nom du fichier (motif dans `tables.py`),
  pas de la date de chargement.
- **Parc véhicule (raw)** : 1 période de contrat (déclaration AEN) ; un véhicule peut porter
  plusieurs lignes. `int_gac__vehicule_code_analytique` ramène à 1 ligne par immatriculation.

---

## Pièges

### Pas de clé unique sur les sinistres
- **Symptôme** : des sinistres sans `n_de_sinistre`.
- **Règle** : le sinistre est identifié par `n_de_sinistre` ou, à défaut, par `reference_gac`.
  Le staging déduplique sur cette clé conditionnelle en gardant le snapshot le plus récent
  (`snapshot_date`, puis `_extracted_at`). Dans un mart ou une jointure, ne jamais utiliser
  `n_de_sinistre` seul : préférer `coalesce(n_de_sinistre, reference_gac)`.
- **Appliqué par** : `stg_gac__sinistres`.

### `collaborateur_fonction_actuelle` contient le code analytique comptable
- **Symptôme** : une colonne nommée comme un poste contient des codes comptables.
- **Règle** : malgré son nom, elle porte le **code analytique (compta)** du véhicule. Elle est
  exposée sous son vrai nom, `code_analytique`. Sont exclus les véhicules sans code analytique et
  les immatriculations non réelles (références internes GAC préfixées `#`). La dernière période
  de contrat fait foi.
- **Appliqué par** : `int_gac__vehicule_code_analytique`.

### En-têtes accentués mutilés dans le raw
- **Symptôme** : colonnes brutes `co_t_global`, `cl_turx`, `r_f_rence_gac`, `immatx`, `pr_nom`,
  `centre_de_co_ts`…
- **Règle** : le normaliseur dlt remplace les accents par des underscores, et un `_` final devient
  `x`. Le staging renomme proprement. Toujours partir du staging.
- **Appliqué par** : `stg_gac__sinistres`, `stg_gac__vehicule`.

### Dates au format `dd/mm/yyyy [hh:mm:ss]`
- **Symptôme** : une date NULL alors que le CSV la porte.
- **Règle** : les dates sont au format français, parsées par `safe.parse_timestamp` /
  `safe.parse_date` : une valeur malformée devient NULL sans erreur. Surveiller
  `date_sinistre is null` si le format du fichier dérive.
- **Appliqué par** : `stg_gac__sinistres`, `stg_gac__vehicule`.

### Coûts castés sans `safe_cast`
- **Symptôme** : le build échoue sur un cast de coût.
- **Règle** : les coûts (`cout_*`, `auto_assurance`, `franchise`) sont castés en `float64`
  directement. Un caractère parasite ou une virgule décimale fait échouer le build plutôt que de
  produire un coût faux. Ne pas passer en `safe_cast` sans journaliser les rejets.
- **Appliqué par** : `stg_gac__sinistres`.

---

## Consommateurs

```bash
dbt ls -s source:gac+
```
