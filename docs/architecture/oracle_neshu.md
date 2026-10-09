# Architecture — Oracle NESHU et LCDP (ERP Distrilog)

| | NESHU | LCDP |
|---|---|---|
| Source dbt | `oracle_neshu` (`models/staging/oracle_neshu/_oracle_neshu__sources.yml`) | `oracle_lcdp` (`models/staging/oracle_lcdp/_oracle_lcdp__sources.yml`) |
| Pipeline dlt | `oracle_neshu` (`ingestion/pipelines/oracle_neshu`) | `oracle_lcdp` (`ingestion/pipelines/oracle_lcdp`) |
| Tables raw | `prod_raw.evs_*` (owner Oracle `EVS`) | `prod_raw.lcdp_*` (owner Oracle `LCDP`) |
| Chargement | `task`, `task_has_product` et leurs jonctions en `merge` sur curseur `modification_date` (propre ou emprunté au parent), toutes rechargées par la purge hebdomadaire ; référentiels en snapshot complet (`replace`) | idem, mais la purge hebdomadaire ne couvre que `task` et `task_has_product` |

Fraîcheur : `docs/freshness.md`. Cadence et orchestration : `docs/pipeline-schedule.md`.

Deux instances du même progiciel, même schéma, mêmes patterns dbt : ce document décrit NESHU,
la dernière section liste ce qui diffère côté LCDP. Stock théorique : `oracle_neshu_gcs.md`.

---

## Grain et clés

| Modèle | Grain | Clé |
|---|---|---|
| `stg_oracle_neshu__task` | 1 tâche, **tous types confondus** | `idtask` |
| `stg_oracle_neshu__task_has_product` | 1 ligne produit d'une tâche | `idtask_has_product` |
| `stg_oracle_neshu__task_has_resources` | 1 ressource affectée à une tâche, par statut | `(idtask, idresources, task_status)` — le couple `(idtask, idresources)` **n'est pas unique** |
| `stg_oracle_neshu__task_has_amount` | 1 montant par tâche, taux, taxe et région de taxe | `(idtask, tax_rate, idtax, idtax_region)` |
| `stg_oracle_neshu__label_has_task` | 1 label posé sur une tâche ; une tâche en porte plusieurs | `(idlabel, idtask)` |
| `stg_oracle_neshu__label_has_thp` | 1 label posé sur une **ligne produit** (mode de paiement télémétrie), à ne pas confondre avec `label_has_product` (catalogue) | `(idlabel, idtask_has_product)` |
| `stg_oracle_neshu__resources` | 1 personne (`idresources_type = 2`) **ou** 1 véhicule (`3`) | `idresources` |
| Référentiels (`company`, `device`, `product`, `contract`, `contact`, `location`…) | 1 entité | `id<entité>` |

- La société d'une tâche est `idcompany_peer` (la tâche n'a pas de `idcompany`), renommée
  `company_id` en intermediate. Une machine porte trois sociétés : `idcompany_customer`,
  `idcompany_owner`, `idcompany_supplier` ; les dims machines se rattachent au client.
- `stg_*__task` mélange tous les types : ne jamais la lire sans filtre `idtask_type`, posé une
  fois dans l'intermediate du type (`int_oracle_neshu__<type>_tasks`) dont part l'aval. Types :
  3 télémétrie · 11 invendus · 13 chargement · 32 passage appro · 101 livraison · 102 facture /
  106 avoir · 120 commande fournisseur · 121 réception · 131 intervention technique ·
  132 commande interne · 161 livraison interne · 162 inventaire · 163 écart d'inventaire ·
  194 pointage.

---

## Pièges

### `code_status_record = '1'` n'est PAS filtré en staging
- **Symptôme** : des tâches dont l'enregistrement n'est pas actif sont comptées dès qu'on lit
  `stg_oracle_*__task` sans filtre.
- **Règle** : le staging expose toutes les tâches. **Tout modèle qui lit `stg_oracle_*__task`
  filtre `code_status_record = '1'`** (chaîne sur la tâche, numérique sur les référentiels).
  Les jonctions ne filtrent rien : elles héritent du filtre par jointure sur la tâche.
- **Appliqué par** : chaque `int_oracle_*__*_tasks` (ex. `int_oracle_neshu__appro_tasks`).

### Le statut de tâche se filtre modèle par modèle
- **Symptôme** : deux modèles du même type de tâche n'ont pas le même volume, ou des tâches
  ANNULE apparaissent dans un intermediate.
- **Règle** : chaque intermediate déclare son filtre `idtask_status`. La plupart des intermediate
  de mouvement gardent FAIT, VALIDE, ANNULE et ANOMALIE et exposent `task_status_code` ; c'est
  au mart de restreindre (souvent FAIT et VALIDE).
- **Appliqué par** : les marts (ex. `fct_neshu__passage_appro`, `fct_lcdp__mouvement_produit`).

### Unités de conditionnement
- **Symptôme** : quantités multipliées ou divisées par le conditionnement (lot, rame…).
- **Règle** : la quantité exploitable vaut `real_quantity × unit_coeff_multi / unit_coeff_div`,
  exposée en `quantity` (`load_quantity` pour le chargement) ; `base_unit_quantity` garde la
  valeur brute. Ne pas reconvertir en aval.
- **Appliqué par** : les `int_oracle_*__*_tasks` à lignes produit.

### La valorisation suit le prix d'achat du catalogue, pas celui de la tâche
- **Symptôme** : la valeur d'un mouvement passé change alors que le mouvement n'a pas bougé.
- **Règle** : `valuation` (`load_valuation`) = quantité ajustée × `purchase_unit_price` de
  `stg_*__product` (exposé `product_unit_price_latest`), lu au build ; le prix de la ligne au
  jour de la tâche est `net_price` (`product_unit_price_task`). Un changement de prix catalogue
  revalorise l'historique des intermediate en `table`, pas celui des incrémentaux.

### Labels (EAV) : pivoter dans la dim, agréger avant de joindre
- **Symptôme** : grain produit doublé après une jointure à `label_has_task` ; attribut
  (région, secteur, actif…) introuvable en colonne.
- **Règle** : un attribut se lit par `label_family` → `label` → `label_has_<entité>`. Le pivot
  est fait une fois dans la dim (`max(case when label_family_code = …)` ; `ISACTIVE` →
  `coalesce(lower(…) = 'yes', false)`) : en aval, joindre la dim, jamais les `label_*`. Les
  labels de tâche s'agrègent au grain tâche avant toute jointure aux lignes produit.
- **Appliqué par** : `dim_neshu__company`, `__device`, `__product`, `__resource`, `__contract` ;
  `int_oracle_neshu__chargement_tasks` pour les labels de tâche.

### Ressources : personnes et véhicules mêlés, plusieurs par tâche
- **Symptôme** : un `roadman_id` et un `roadman_code` qui désignent deux personnes différentes
  sur une tâche en binôme.
- **Règle** : filtrer `idresources_type` (2 personne, 3 véhicule) et retenir **une ligne
  entière** par tâche (`row_number()` sur le plus petit `idresources`), jamais des `min()`
  indépendants par colonne. Le `gea_code` des personnes vient du seed
  `ref_oracle_neshu__roadman_gea`, joint dans `dim_neshu__resource`.
- **Appliqué par** : `int_oracle_neshu__appro_tasks_enriched`, `int_oracle_lcdp__appro_tasks_enriched`.

### `contact_has_device` relie une machine à du personnel interne
- **Règle** : le « contact » est un roadman ou un technicien, pas un client. Seul lien vers
  `resources` : `contact.code = resources.code`. Aucune date : affectation courante, non historisable.
- **Appliqué par** : `dim_lcdp__device` (roadman affecté).

### Une suppression dans l'ERP n'atteint pas les staging incrémentaux
- **Règle** : Distrilog n'a pas de suppression logique sur le transactionnel. La purge
  hebdomadaire retire du raw les lignes supprimées, mais un staging incrémental fait un `merge`
  sans suppression : la ligne y reste jusqu'au prochain `--full-refresh` du modèle. Sur les
  référentiels, `code_status_record = -1` marque un enregistrement supprimé côté ERP.
- **Appliqué par** : `dim_neshu__*`, qui écartent `code_status_record = -1`.

### Staging incrémentaux : fenêtre de 7 jours et type du raw
- **Règle** : `task`, `task_has_product`, `task_has_amount` et `label_has_thp` sont en `merge`
  sur `updated_at > max(updated_at)` ou `updated_at` des 7 derniers jours ; une correction plus
  ancienne demande un `--full-refresh`. Une colonne non castée hérite du type du raw : caster,
  et valider un changement de type sans `--full-refresh` (cf. `CLAUDE.md` § Frontières).

---

## Règles métier

- **Signe du CA** : `task_type_has_config` (`idconfig = 'coefficient'`) vaut +1 sur FACT
  CLIENT (102) et −1 sur AVOIR (106). C'est la seule source de la règle ; sans elle un avoir
  s'ajoute au CA. Appliqué par `int_oracle_neshu__facturation_tasks`.
- **XML** : le staging du contrat en extrait `NOMBRE_COLLAB` et `ENGAGEMENT` (macro
  `decoder_entites_xml`) ; celui de la tâche est exposé brut et parsé en aval (`/ZONE/COUTRM`).
- **Historique** : les dims sont à l'état courant. L'historique passe par les snapshots SCD2
  `snap_oracle_neshu__company`, `__device` et `__valo_parc_machines` (Cloud Workflows).

---

## LCDP : différences

Tout ce qui précède s'applique à `oracle_lcdp` (`stg_oracle_lcdp__*`, `int_oracle_lcdp__*`), sauf :

- **Types de tâche propres** : 130 appel SAV, 30 comptage, 296 entrée et 297 sortie fabrication.
- **Nommage** : `int_oracle_lcdp__inter_technique_tasks` ; côté NESHU le modèle s'appelle
  réellement `int_oracle_neshu__inter_techinique_tasks` (coquille dans le nom).
- **Pas de dimension contrat** : ni `dim_lcdp__contract`, ni staging de `label_has_contract`.
- **Staging incrémentaux** : `task` et `task_has_product` seulement ; `task_has_amount` est une
  table, `label_has_thp` et `task_type_has_config` n'existent pas.
- **Labels des dims** : `dim_lcdp__company`, `__device` et `__product` lisent les vues Oracle
  `v_label_*` (`stg_oracle_lcdp__label_company`, `__label_device`, `__label_product`), qui
  portent déjà famille et libellé FR ; `dim_lcdp__resource` pivote `label_has_resources`.
- **Enregistrements supprimés** : les dims LCDP n'écartent pas `code_status_record = -1`.
- **Jonctions non purgées** : une liaison supprimée dans `label_has_task`, `task_has_resources`
  ou `task_has_amount` reste dans le raw. Un comptage à travers ces jonctions peut être gonflé :
  dédupliquer au grain tâche.
- **`comments_self`** : des octets non UTF-8 hérités sont remplacés à l'extraction par `U+FFFD`
  (repérables par `like '%�%'`).
- **Snapshot** : `snap_lcdp__device` seul.

---

## Consommateurs

`dbt ls -s source:oracle_neshu+` et `dbt ls -s source:oracle_lcdp+`.
