# Architecture — GAC

| | |
|---|---|
| Source dbt | `gac` (`models/staging/gac/_gac__sources.yml`) |
| Pipeline | `ingestion/pipelines/gac` : CSV déposés par GAC sur le SFTP EVS |
| Chargement | sinistres et historique des affectations : `merge` / `delete-insert` sur `snapshot_date`, les fichiers quotidiens s'accumulent ; parc véhicule : `replace`, le fichier écrasé est l'état courant, sans historique |

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).

GAC est le loueur de la flotte de véhicules. Il dépose trois CSV (`;`, latin-1) : le suivi des
**sinistres** (un fichier daté, cumulatif), le **parc véhicule** (un fichier unique) et
l'**historique des affectations** (un fichier daté, fenêtre glissante de 12 mois). Pas de
dimension dédiée : véhicule, collaborateur et centre de coûts restent dénormalisés dans le fait
des sinistres. L'historique des affectations sert la ventilation analytique des factures de
véhicules (`app_compta__vehicule_imputation`).

---

## Clés et grain

- **Sinistres (raw)** : 1 sinistre dans 1 snapshot daté, sans clé de ligne. La date du snapshot
  (`snapshot_date`) vient de la date que GAC écrit dans le nom du fichier (motif dans `tables.py`),
  pas de la date de chargement.
- **Parc véhicule (raw)** : 1 contrat × 1 ligne de contrat type, en pratique une prime
  d'assurance annuelle ; un véhicule peut porter plusieurs lignes.
  `int_gac__vehicule_code_analytique` ramène à 1 ligne par immatriculation.
- **Historique des affectations (raw)** : 1 évènement (période de disponibilité : affecté,
  disponible, en maintenance, indisponible) d'un contrat dans 1 fichier daté. Clé :
  (`snapshot_date`, `contrat_id_gac`, `date_d_effet`). Les périodes d'un contrat se suivent, la
  fin est incluse, vide = en cours.

---

## Pièges

### Pas de clé unique sur les sinistres
- **Symptôme** : des sinistres sans `n_de_sinistre`.
- **Règle** : le sinistre est identifié par `n_de_sinistre` ou, à défaut, par `reference_gac`.
  Le staging déduplique sur cette clé conditionnelle en gardant le snapshot le plus récent
  (`snapshot_date`, puis `_extracted_at`). Dans un mart ou une jointure, ne jamais utiliser
  `n_de_sinistre` seul : préférer `coalesce(n_de_sinistre, reference_gac)`.
- **Appliqué par** : `stg_gac__sinistres`.

### `fonction_actuelle` contient le code analytique comptable du collaborateur
- **Symptôme** : une colonne nommée comme un poste contient des codes comptables
  (`collaborateur_fonction_actuelle` dans le parc, `fonction_actuelle` dans l'historique).
- **Règle** : malgré son nom, elle porte le **code analytique (compta)** saisi dans la fiche du
  collaborateur. Le véhicule n'en a pas en propre : un véhicule sans collaborateur n'a pas de
  code. Exposée sous son vrai nom, `code_analytique`.
- **Appliqué par** : `int_gac__vehicule_code_analytique`, `app_compta__vehicule_imputation`.

### L'historique n'exporte que le code actuel du collaborateur
- **Symptôme** : un évènement d'il y a six mois porte le code qu'a le collaborateur aujourd'hui.
- **Règle** : GAC recopie le code actuel sur tous les évènements, passés compris. Le code à une
  date se lit dans le fichier de ce jour-là : seuls les fichiers empilés le gardent. Un code vide
  est un oubli de saisie, le code connu le plus proche s'applique.
- **Appliqué par** : `app_compta__vehicule_imputation`.

### L'historique des affectations glisse et se corrige
- **Symptôme** : un évènement disparaît du dernier fichier, ou change de date d'effet d'un
  fichier à l'autre.
- **Règle** : le fichier le plus récent d'un contrat fait foi sur la période qu'il couvre ; un
  évènement plus ancien, sorti de la fenêtre de 12 mois, garde sa dernière version. Les
  évènements dont la fin précède l'effet ne couvrent aucun jour et sont écartés.
- **Appliqué par** : `app_compta__vehicule_imputation`.

### En-têtes accentués mutilés dans le raw
- **Symptôme** : colonnes brutes `co_t_global`, `cl_turx`, `r_f_rence_gac`, `immatx`, `pr_nom`,
  `centre_de_co_ts`…
- **Règle** : le normaliseur dlt remplace les accents par des underscores, et un `_` final devient
  `x`. Le staging renomme proprement. Toujours partir du staging. L'historique des affectations
  y échappe : l'ingestion normalise ses en-têtes sans accents.
- **Appliqué par** : `stg_gac__sinistres`, `stg_gac__vehicule`.

### Dates au format `dd/mm/yyyy [hh:mm:ss]`
- **Symptôme** : une date NULL alors que le CSV la porte.
- **Règle** : les dates sont au format français, parsées par `safe.parse_timestamp` /
  `safe.parse_date` : une valeur malformée devient NULL sans erreur. Surveiller
  `date_sinistre is null` si le format du fichier dérive.
- **Appliqué par** : `stg_gac__sinistres`, `stg_gac__vehicule`, `stg_gac__historique_affectation`.

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
