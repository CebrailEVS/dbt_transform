# Architecture — Nespresso Commerce (`nesp_co`)

| | |
|---|---|
| Source dbt | `nesp_co` — `models/staging/nesp_co/_nesp_co__sources.yml` |
| Pipelines dlt | `ingestion/pipelines/nesp_co` (activités, opportunités) et `ingestion/pipelines/nesp_client` (base clients), moteur commun `shared/sftp_evs`, qui charge des classeurs Excel depuis le SFTP EVS. Activités et opportunités arrivent par mail : le pipeline `nesp_co` relève la boîte et dépose lui-même les classeurs sur le SFTP avant de les charger |
| Chargement | activités et opportunités : `merge` en `delete-insert` sur `snapshot_date` (date lue dans le nom du classeur), un jour relu écrase le sien et le raw garde un export par jour. Base clients : `merge` sur `third`, la version du classeur le plus récemment modifié gagne |

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).
Le pipeline `nesp_client` n'a pas de scheduler (`infra/workflows_el.tf`) : la base clients
n'avance qu'à chaque déclenchement manuel.

Les activités et opportunités viennent du CRM Nespresso C4C ; chaque classeur est un export
complet sur une fenêtre glissante. La base clients est le référentiel des clients Nespresso
suivis par EVS (`third`), qui fait le pont avec C4C.

---

## Pièges

**Sentinelle `'#'`.** Les exports C4C écrivent `'#'` pour une valeur absente. Règle : chaque
colonne de `stg_nesp_co__activite` et `stg_nesp_co__opportunite` passe par
`nullif(<col>, '#')` avant tout cast ; un `'#'` oublié fait échouer le cast. La base clients
n'utilise pas cette sentinelle.

**Colonnes positionnelles `unnamed_*`.** Des colonnes de l'export n'ont pas d'en-tête et
arrivent sous un nom de position (`unnamed_1`, `unnamed_12`…), épinglé dans `tables.py` du
pipeline. Le staging les renomme (`c4c_id_commercial`, `c4c_id_main_account`, `c4c_id_account`,
`c4c_id_campaign`). Si Nespresso réordonne ses colonnes, les identifiants se décalent ; aucun
test ne porte sur ces colonnes, seul le cast `int64` des identifiants de compte et de campagne
peut échouer.

**`activity_id` seul n'identifie pas une activité.** C4C numérote à part les appels et les
rendez-vous : un même numéro peut désigner un appel et un rendez-vous sans rapport entre eux
(comptes, dates et commerciaux différents). La clé d'une activité est le couple
(`activity_id`, `activity_type`), unique dans chaque export : c'est celle de
`stg_nesp_co__activite`, et un test sur la source la vérifie journée par journée.

**Le couple reste la clé jusqu'au mart.** `int_nesp_co__activites` et `fct_commerce__activite`
gardent une ligne par activité, (`act_id`, `act_type`) : un appel et un rendez-vous de même
numéro y figurent tous les deux. Règle : compter les lignes, pas les `act_id` distincts, et ne
jamais joindre sur `act_id` seul.

**Dernier état seulement.** Le staging garde la version la plus récente de chaque activité et
opportunité : une opportunité gagnée n'y figure qu'une fois, à son dernier statut. L'historique
des transitions n'existe que dans le raw, qui conserve un export par `snapshot_date`. Une ligne
qui ne figure plus dans l'export, quelle qu'en soit la raison, reste en staging à sa dernière
version connue : une sortie de la fenêtre ou une suppression côté C4C ne s'y voit pas.

**Ordonner les versions sur la journée, pas sur le chargement.** La version la plus récente est
celle du dernier `snapshot_date` (`file_date` en staging). `_extracted_at` ne les départage pas :
un run de rattrapage charge plusieurs journées sous la même valeur.

**Lignes de total `Result`.** L'export des opportunités contient des sous-totaux C4C
(`opportunity = 'Result'`). Règle : filtrés dans `stg_nesp_co__opportunite`.

**Exports d'opportunités sans rôle ni EZ.** Certains exports laissent `role_account` et
`zeq_zenius_equivalent` vides sur toutes leurs lignes. Règle : `stg_nesp_co__opportunite`
garde la version du dernier export qui porte ces deux colonnes, et ne retient un export sans
elles que pour une opportunité qu'aucun autre ne porte : son rôle et son EZ restent alors vides.

**Périmètre de l'export fixé par Nespresso.** Le filtre de dates et la Sales Unit de l'export
des opportunités sont réglés côté C4C ; une colonne peut arriver vide sans que rien n'échoue.
Un changement de périmètre se voit aux volumes, pas aux tests. Il se corrige côté Nespresso,
pas dans le pipeline ni dans dbt. Les exports au périmètre différent sont datés dans
`ingestion/pipelines/nesp_co/README.md`, § « Lire les données ».

**Colonnes dédoublées par type d'activité.** Le libellé, la date de début et l'auteur d'une
activité sont dans des colonnes distinctes selon le type (appel, rendez-vous, tâche). Règle :
`int_nesp_co__activites` les unifie en `act_nom`, `act_date_debut`, `act_cree_par` par `case`
sur `activity_type`.

**Libellés traduits en dur.** Les intermediates traduisent les libellés anglais de C4C par
`case when`. Une valeur nouvelle côté source remonte telle quelle, en anglais : ajouter le
`when` correspondant.

**`chance_of_success` en texte.** Il arrive en pourcentage texte, virgule décimale possible.
Règle : le staging retire `%` et remplace la virgule avant le cast ; `int_nesp_co__opportunites`
divise par 100 (`opp_probabilite`, ratio 0–1).

**Base clients : plusieurs classeurs dans un même run.** Le pipeline étant manuel, plusieurs
dépôts arrivent souvent ensemble et diffèrent entre eux. Règle : la version la plus récente
d'un client est celle du classeur le plus récemment modifié (`_fichier_modifie_le`), jamais
l'heure de chargement ; appliqué par dlt au merge (`dedup_sort`) et répété dans
`stg_nesp_co__client`.

---

## Jointure inter-sources

- **C4C → client EVS** : `third` s'extrait de l'identifiant Nessoft (`FR_<third>`, regex
  `FR_(\d+)`) porté par les activités et opportunités. Les faits `fct_commerce__activite` et
  `fct_commerce__opportunite` ne portent pas `third` mais l'identifiant brut (`act_id_nessoft`,
  `opp_id_compte`) : la jointure au client passe par la même extraction. Appliqué par
  `int_nesp_co__clients_enrichis`.
- **Compte C4C d'un client** : aucun lien natif ; déduit des opportunités et activités qui le
  citent, le plus récent gagne, `NULL` sans activité ni opportunité. Appliqué par
  `int_nesp_co__clients_enrichis`.

Les autres règles (groupe du client, semaine ISO) sont dans les `int_nesp_co__*` et leurs
descriptions YAML.

---

## Consommateurs

```bash
dbt ls -s source:nesp_co+ --resource-type model
```
