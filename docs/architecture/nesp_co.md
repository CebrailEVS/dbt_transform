# Architecture — Nespresso Commerce (`nesp_co`)

| | |
|---|---|
| Source dbt | `nesp_co` — `models/staging/nesp_co/_nesp_co__sources.yml` |
| Pipelines dlt | `ingestion/pipelines/nesp_co` (activités, opportunités) et `ingestion/pipelines/nesp_client` (base clients), moteur commun `shared/sftp_evs` |
| Fichiers source | classeurs Excel déposés sur le SFTP EVS (`nespresso/commerce/incoming`) |
| Tables raw | `prod_raw.nesp_co_activite`, `prod_raw.nesp_co_opportunite`, `prod_raw.nespresso_base_client` |
| Fraîcheur | [`docs/freshness.md`](../freshness.md) |
| Cadence | [`docs/pipeline-schedule.md`](../pipeline-schedule.md) ; les deux workflows enchaînent leur extraction et `dbt build -s source:nesp_co+` |

**Modes de chargement** :

| Table raw | Pipeline | Mode | Conséquence |
|---|---|---|---|
| `nesp_co_activite`, `nesp_co_opportunite` | `nesp_co`, planifié | `merge` en `delete-insert` sur `snapshot_date` (date lue dans le nom du classeur) | chaque classeur est un export complet sur une fenêtre glissante ; un jour relu écrase son propre jour, les autres restent. Le raw garde un export par jour |
| `nespresso_base_client` | `nesp_client`, **déclenché à la main**, sans scheduler | `merge` sur `third`, la version du classeur le plus récent gagne (`dedup_sort` sur `_fichier_modifie_le`) | la base clients n'avance qu'à chaque dépôt manuel : son âge est normal, pas une panne |

Les activités et opportunités viennent du CRM Nespresso C4C ; la base clients est le
référentiel des clients Nespresso suivis par EVS (`third`), qui fait le pont avec C4C.

---

## Grain et clés

| Modèle de staging | Grain | Clé | Lien |
|---|---|---|---|
| `stg_nesp_co__activite` | 1 activité commerciale (appel, rendez-vous, tâche), dernier état connu | `activity_id` | `nessoft_id_main_account` = `FR_<third>` ; `c4c_id_main_account` (compte C4C) |
| `stg_nesp_co__opportunite` | 1 opportunité, dernier état connu | `opportunity_id` | `nessoft_id_account` = `FR_<third>` ; `c4c_id_account` |
| `stg_nesp_co__client` | 1 client | `third` | aucun identifiant C4C natif |

---

## Pièges

**Sentinelle `'#'`.** Les exports C4C écrivent `'#'` pour une valeur absente. Règle : chaque
colonne de `stg_nesp_co__activite` et `stg_nesp_co__opportunite` passe par
`nullif(<col>, '#')` avant tout cast ; un `'#'` oublié fait échouer le cast. La base clients
n'utilise pas cette sentinelle.

**Colonnes positionnelles `unnamed_*`.** Des colonnes de l'export n'ont pas d'en-tête et
arrivent sous un nom de position (`unnamed_1`, `unnamed_12`…), épinglé dans `tables.py` du
pipeline. Le staging les renomme (`c4c_id_commercial`, `c4c_id_main_account`, `c4c_id_account`,
`c4c_id_campaign`). Si Nespresso réordonne ses colonnes, les identifiants se décalent sans
erreur : seuls les tests `not_null` sur les identifiants le révèlent.

**`activity_id` n'est pas unique dans un même export.** Un même identifiant peut porter deux
lignes dans un classeur ; c'est pourquoi le pipeline merge sur la journée et non sur la ligne.
Le staging dédoublonne sur `activity_id` par `extracted_at` décroissant : entre deux lignes du
même export, la ligne gardée est arbitraire.

**Dernier état seulement.** Le staging garde la version la plus récente de chaque activité et
opportunité : une opportunité passée de `Open` à `Won` n'y figure qu'une fois. L'historique
des transitions n'existe que dans le raw, qui conserve un export par `snapshot_date`.

**Lignes de total `Result`.** L'export des opportunités contient des sous-totaux C4C
(`opportunity = 'Result'`). Règle : filtrés dans `stg_nesp_co__opportunite`.

**Périmètre de l'export fixé par Nespresso.** Le filtre de dates et la Sales Unit de l'export
des opportunités sont réglés côté C4C ; une colonne peut arriver vide sans que rien n'échoue.
Un changement de périmètre se voit aux volumes, pas aux tests. Il se corrige côté Nespresso,
pas dans le pipeline ni dans dbt.

**Colonnes dédoublées par type d'activité.** Le libellé, la date de début et l'auteur d'une
activité sont dans des colonnes distinctes selon le type (`phone_call` / `appointment` /
tâche). Règle : `int_nesp_co__activites` les unifie en `act_nom`, `act_date_debut`,
`act_cree_par` par `case` sur `activity_type`.

**Libellés traduits en dur.** Les intermediates traduisent les libellés anglais de C4C
(`Phone Call` → `Appel téléphonique`, `Won` → `Gagné`…) par `case when`. Une valeur nouvelle
côté source remonte telle quelle, en anglais : ajouter le `when` correspondant.

**`chance_of_success` en texte.** Il arrive en `"75%"` ou `"75,5%"`. Règle : le staging retire
`%` et remplace la virgule avant le cast ; `int_nesp_co__opportunites` divise par 100
(`opp_probabilite`, ratio 0–1).

**Base clients : plusieurs classeurs dans un même run.** Le pipeline étant manuel, plusieurs
dépôts arrivent souvent ensemble et diffèrent entre eux. Règle : la version la plus récente
d'un client est celle du classeur le plus récemment modifié (`_fichier_modifie_le`), jamais
l'heure de chargement ; appliqué par dlt au merge et répété dans `stg_nesp_co__client`.

---

## Règles métier et leur source

| Règle | Source | Appliquée dans |
|---|---|---|
| Lien C4C → client EVS : `third` extrait de `nessoft_id` (`FR_<third>`, regex `FR_(\d+)`). Les faits `fct_commerce__activite` et `fct_commerce__opportunite` ne portent pas `third` mais l'identifiant brut (`act_id_nessoft`, `opp_id_compte`) : la jointure au client passe par la même extraction | format de l'identifiant Nessoft | `int_nesp_co__clients_enrichis` |
| Compte C4C d'un client : déduit des opportunités et activités qui le citent, le plus récent gagne ; `NULL` pour un client sans activité ni opportunité C4C | déduction interne, aucun lien natif | `int_nesp_co__clients_enrichis` |
| Groupe du client : secteur contenant `FID` → région en majuscules ; sinon `RS <catégorie client>` | règle commerce EVS | `int_nesp_co__clients_enrichis` |
| Semaine d'une activité au format `SS.AAAA` (semaine ISO) | reporting commerce | `int_nesp_co__activites` |

---

## Consommateurs

Les marts `models/marts/commerce/`. `fct_commerce__machine_intervention` n'y croise pas les
opportunités : il lit les interventions `nesp_tech` (`int_nesp_tech__interventions_dedup`) et ne
prend à `nesp_co` que le client, via `dim_commerce__client`. Liste à jour :

```bash
dbt ls -s source:nesp_co+ --resource-type model
```
