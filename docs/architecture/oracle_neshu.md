# Architecture — ERP Oracle Distrilog (NESHU, LCDP, Nayka)

| Instance | Source dbt | Pipeline dlt | Tables raw |
|---|---|---|---|
| NESHU | `oracle_neshu` (`models/staging/oracle_neshu/_oracle_neshu__sources.yml`) | `ingestion/pipelines/oracle_neshu` | `prod_raw.evs_*` |
| LCDP | `oracle_lcdp` (`models/staging/oracle_lcdp/_oracle_lcdp__sources.yml`) | `ingestion/pipelines/oracle_lcdp` | `prod_raw.lcdp_*` |
| Nayka | `oracle_nayka` (`models/staging/oracle_nayka/_oracle_nayka__sources.yml`) | `ingestion/pipelines/oracle_nayka` | `prod_raw.nayka_*` |

**Chargement** (moteur commun `ingestion/shared/oracle_erp`, périmètre dans chaque `tables.py`) :
transactionnel en `merge` sur `modification_date`, propre ou emprunté à la tâche pour les
jonctions, dont les lignes relues remplacent celles de la tâche ; référentiels en `replace`.
Toutes les tables en `merge`, jonctions comprises, sont vidées et rechargées par la purge
hebdomadaire, dont le workflow reconstruit ensuite `source:<instance>+` en `--full-refresh`.
NESHU seule : `task_historic` en ajout seul, le raw garde ce que l'ERP purge
([`staging.md` § 5](../conventions/staging.md)).

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).
Stock théorique : [`oracle_neshu_gcs.md`](oracle_neshu_gcs.md).

Trois instances du même progiciel : même schéma, mêmes pièges. Ce document décrit NESHU ; les
deux dernières sections listent ce qui diffère pour LCDP et pour Nayka.

---

## Clés et grain

- `task_has_resources` : clé `(idtask, idresources, task_status)`, le couple `(idtask, idresources)`
  **n'est pas unique** ; `task_has_amount` : une ligne par tâche, taux, taxe et région de taxe.
  Ces clés sont celles déclarées par Oracle : ne pas les rétrécir.
- `label_has_thp` : label posé sur une **ligne produit** (mode de paiement télémétrie), à ne pas
  confondre avec `label_has_product` (catalogue).
- La société d'une tâche est `idcompany_peer` (pas de `idcompany`), renommée `company_id` en
  intermediate. Une machine porte trois sociétés (`idcompany_customer`, `idcompany_owner`,
  `idcompany_supplier`) ; les dims machines se rattachent au client.
- `contact_has_device` relie une machine à du personnel interne (roadman, technicien), pas à un
  client. Seul lien vers `resources` : `contact.code = resources.code`. Aucune date : affectation
  courante, non historisable (appliqué par `dim_lcdp__device`).

---

## Pièges

### `stg_*__task` mélange tous les types de tâche
- **Règle** : ne jamais la lire sans filtre `idtask_type`, posé une fois dans l'intermediate du
  type (`int_oracle_<erp>__<type>_tasks`) dont part l'aval. Filtrer sur l'identifiant, pas sur le
  libellé `code` de `stg_*__task_type`, qui n'est pas unique. Les types standard de l'ERP ont le
  même `idtask_type` d'une instance à l'autre.
- **Appliqué par** : chaque `int_oracle_*__*_tasks` ; son filtre fait foi pour son type.

### `code_status_record = '1'` n'est PAS filtré en staging
- **Symptôme** : des tâches dont l'enregistrement n'est pas actif sont comptées.
- **Règle** : **tout modèle qui lit `stg_oracle_*__task` filtre `code_status_record = '1'`**
  (chaîne sur la tâche, numérique sur les référentiels). Les jonctions héritent du filtre par
  jointure sur la tâche.
- **Appliqué par** : chaque `int_oracle_*__*_tasks` (ex. `int_oracle_neshu__appro_tasks`).

### Le statut de tâche se filtre modèle par modèle
- **Symptôme** : deux modèles du même type n'ont pas le même volume, ou des tâches ANNULE
  apparaissent dans un intermediate.
- **Règle** : chaque intermediate déclare son filtre `idtask_status`. La plupart des intermediate
  de mouvement gardent FAIT, VALIDE, ANNULE et ANOMALIE et exposent `task_status_code` ; c'est
  au mart de restreindre.
- **Appliqué par** : les marts (ex. `fct_neshu__passage_appro`, `fct_lcdp__mouvement_produit`).

### Les dates Oracle sont l'heure de Paris, sans fuseau
- **Règle** : le staging les convertit par `paris_vers_utc` ; un jour métier se calcule en heure
  de Paris ([`CONVENTIONS.md` § Fuseau horaire](../../CONVENTIONS.md#fuseau-horaire)).

### Unités de conditionnement
- **Symptôme** : quantités multipliées ou divisées par le conditionnement (lot, rame…).
- **Règle** : la quantité exploitable vaut `real_quantity × unit_coeff_multi / unit_coeff_div`,
  exposée en `quantity` (`load_quantity` pour le chargement) ; `base_unit_quantity` garde la
  valeur brute. Ne pas reconvertir en aval.
- **Libellé du conditionnement** (« CARTON DE 24 ») : `idproduct_unit` de la ligne →
  `stg_*__product_unit.idstring` → `stg_*__string` filtré sur `langage_code = 'fr_FR'` ; `string`
  porte une ligne par langue, sans ce filtre la jointure multiplie les lignes.
- **Appliqué par** : les `int_oracle_*__*_tasks` à lignes produit.

### La valorisation suit le prix d'achat du catalogue, pas celui de la tâche
- **Symptôme** : la valeur d'un mouvement passé change alors que le mouvement n'a pas bougé.
- **Règle** : `valuation` (`load_valuation`) = quantité ajustée × `purchase_unit_price` de
  `stg_*__product` (`product_unit_price_latest`), lu au build ; le prix de la ligne au jour de la
  tâche est `net_price` (`product_unit_price_task`). Un changement de prix catalogue revalorise
  l'historique des intermediate en `table` au build suivant, celui des incrémentaux à la
  reconstruction de la purge hebdomadaire.

### Labels (EAV) : pivoter dans la dim, agréger avant de joindre
- **Symptôme** : grain produit doublé après une jointure à `label_has_task` ; attribut
  (région, secteur, actif…) introuvable en colonne.
- **Règle** : un attribut se lit par `label_family` → `label` → `label_has_<entité>`. Le pivot
  est fait une fois dans la dim (`max(case when label_family_code = …)` ; `ISACTIVE` →
  `coalesce(lower(…) = 'yes', false)`) : en aval, joindre la dim, jamais les `label_*`. Les
  labels de tâche s'agrègent au grain tâche avant toute jointure aux lignes produit.
- **Appliqué par** : les `dim_neshu__*` ; `int_oracle_neshu__chargement_tasks` pour les labels de tâche.

### Ressources : personnes et véhicules mêlés, plusieurs par tâche
- **Symptôme** : un `roadman_id` et un `roadman_code` qui désignent deux personnes différentes
  sur une tâche en binôme.
- **Règle** : filtrer `idresources_type` (2 personne, 3 véhicule) et retenir **une ligne
  entière** par tâche (`row_number()` sur le plus petit `idresources`), jamais des `min()`
  indépendants par colonne. Le `gea_code` des personnes vient du seed
  `ref_oracle_neshu__roadman_gea`, joint dans `dim_neshu__resource`.
- **Appliqué par** : `int_oracle_neshu__appro_tasks_enriched`, `int_oracle_lcdp__appro_tasks_enriched`.

### Une suppression dans l'ERP n'atteint l'aval qu'à la purge hebdomadaire
- **Règle** : Distrilog n'a pas de suppression logique sur le transactionnel et un `merge` ne
  retire rien. Une tâche supprimée reste au raw, donc dans tout l'aval, jusqu'à la purge ; les
  modèles incrémentaux ne la perdent qu'à la reconstruction `--full-refresh` que lance ce
  workflow (`infra/workflows/pipeline-oracle-<instance>-purge.yaml`). Sur les référentiels,
  `code_status_record = -1` marque un enregistrement supprimé côté ERP.
- **Appliqué par** : les `dim_neshu__*`, qui écartent `code_status_record = -1`.

### Staging incrémentaux : fenêtre de 7 jours et type du raw
- **Règle** : les gros staging de tâches sont incrémentaux (le `config()` fait foi) : `merge` sur
  `updated_at > max(updated_at)` ou `updated_at` des 7 derniers jours. Une ligne modifiée au raw
  sans que son `updated_at` avance n'est reprise qu'à une reconstruction complète. Une colonne
  non castée hérite du type du raw : caster, et valider un changement de type sans
  `--full-refresh` (cf. `CLAUDE.md` § Frontières).

---

## Règles métier

- **Signe du CA** : un avoir (106) se retranche de la facture (102).
  `int_oracle_neshu__facturation_tasks` lit le coefficient dans `task_type_has_config`
  (`idconfig = 'coefficient'`) ; `task_type.coeff` porte le même sens au raw mais n'est pas repris
  par le staging NESHU. Un type sans coefficient compte en positif.
- **XML** : le staging du contrat en extrait `NOMBRE_COLLAB` et `ENGAGEMENT` (macro
  `decoder_entites_xml`) ; celui de la tâche est exposé brut et parsé en aval (`/ZONE/COUTRM`).
- **Historique** : les dims sont à l'état courant. L'historique passe par les snapshots SCD2
  `snap_oracle_neshu__company`, `__device` et `__valo_parc_machines` (Cloud Workflows).

---

## LCDP : différences

Tout ce qui précède s'applique à `oracle_lcdp` (`stg_oracle_lcdp__*`, `int_oracle_lcdp__*`), sauf :

- **Types de tâche propres** (appel SAV, comptage, fabrication) : voir `models/intermediate/oracle_lcdp/`.
- **Transactionnel plus étroit** : ni labels de ligne produit, ni configuration du signe, ni
  historique des tâches (`tables.py`).
- **Pas de dimension contrat** : ni `dim_lcdp__contract`, ni staging de `label_has_contract`.
- **Labels des dims** : `dim_lcdp__company`, `__device` et `__product` lisent les vues Oracle
  `v_label_*` (`stg_oracle_lcdp__label_company`, `__label_device`, `__label_product`), qui
  portent déjà famille et libellé ;
  `dim_lcdp__resource` pivote `label_has_resources`.
- **Enregistrements supprimés** : les dims LCDP n'écartent pas `code_status_record = -1`.
- **`comments_self`** : des octets non UTF-8 hérités sont remplacés à l'extraction par `U+FFFD`
  (repérables par `like '%�%'`).
- **Snapshot** : `snap_lcdp__device` seul.

---

## Nayka : différences

Transactionnel identique à LCDP, verrouillé par `test_le_transactionnel_est_identique_a_lcdp`
(`ingestion/pipelines/oracle_nayka/tests/`) : une table en `merge` se modifie dans les deux
`tables.py`. Les pièges ci-dessus s'appliquent, sauf :

- **Staging tout en `table`** : ni fenêtre de 7 jours ni `MERGE` sur un type qui change ; une
  ligne purgée du raw disparaît au build suivant.
- **`code_status_record` numérique partout, tâche comprise** : filtrer `= 1`, pas `= '1'`.
- **`task_type`** expose `code_type` (type fonctionnel standard de l'ERP) et `coeff` (sens des
  montants, −1 sur un avoir) ; `task_type_has_config` n'est pas extraite.
- **`string` n'est pas extraite** : les colonnes `idstring*` ne mènent nulle part. Les labels des
  référentiels se lisent par les vues `v_label_*`, comme chez LCDP.
- **Table vide à la source = table absente du raw** (dlt n'en crée pas) : vérifier qu'elle existe
  avant d'écrire un staging.
- **Tout modèle qui lit directement le staging Nayka** applique lui-même les règles ci-dessus
  (`code_status_record`, statut, unités, `-1` sur les référentiels).
- **Snapshot** : `snap_oracle_nayka__device` (parc d'équipements), qui lit
  `int_oracle_nayka__device` et non une dim : Nayka n'a pas de mart.

---

## Consommateurs

`dbt ls -s source:oracle_neshu+`, `dbt ls -s source:oracle_lcdp+` et `dbt ls -s source:oracle_nayka+`.
