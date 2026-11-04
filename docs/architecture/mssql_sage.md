# Architecture — MSSQL Sage (`mssql_sage`)

| | |
|---|---|
| Source dbt | `mssql_sage` — `models/staging/mssql_sage/_mssql_sage__sources.yml` |
| Pipeline dlt | `ingestion/pipelines/mssql_sage` (SQL Server) ; périmètre et mode de chaque table dans `tables.py` |
| Chargement | `replace` par défaut : le raw ne contient que ce qui existe dans Sage au dernier run, une suppression ou une transformation de document y disparaît. `append` pour `f_artstock` et `f_lotserie` : Sage n'a que l'état courant, chaque run ajoute une photo complète, identifiée par `_extracted_at` ; le staging garde toutes les photos, le choix d'une photo se fait en aval |

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).

Le dossier Sage porte deux domaines qui ne partagent que la base :

- **Comptabilité, toutes BU** : écritures générales et analytiques, plan comptable, sections
  analytiques. Alimente le P&L par BU (`models/marts/finance/`).
- **Gestion commerciale et stock, BU Nunshen** : documents, articles, stock, lots,
  nomenclatures, tiers. Alimente l'application Cockpit Supply (`app_cockpit__nunshen_*`).

---

## Clés et grain

Les clés sont testées dans `_mssql_sage__models.yml`. Cas non évidents :

- `f_ecriturea` : 0 à N lignes de ventilation analytique par écriture (`ec_no`).
- `f_docentete` / `f_docligne` : un document se désigne par `(do_type, do_piece)` ; `do_piece`
  seul n'est pas unique.
- `cb_marq` est l'identifiant technique Sage de chaque ligne. Sur les tables en `append`, il
  n'est unique **qu'à l'intérieur d'une photo**.
- `f_lotserie` : une ligne par mouvement de lot (entrée `dl_no_in`, sortie `dl_no_out`, 0 tant
  que le lot n'est pas sorti) ; aucune clé unique déclarée par Sage.

---

## Pièges

**Écriture en double après une modification dans Sage.** Sage crée parfois un nouveau
`cb_marq` lors d'une mise à jour au lieu de modifier la ligne. Règle : dédoublonner sur la clé
métier en gardant la version la plus récente. Appliqué dans `stg_mssql_sage__f_ecriturec`
(`ec_no`) et `stg_mssql_sage__f_ecriturea` (`ec_no, n_analytique, ea_ligne`) ; d'autres staging
portent le même `qualify` par prudence.

**Date `1753-01-01`.** Sage écrit la date minimale de SQL Server pour « pas de date ». Règle :
`nullif(<col>, timestamp('1753-01-01'))` en staging, à reproduire sur toute nouvelle colonne
date.

**Mauvais mois dans le P&L.** `ec_date` n'est pas la date de rattachement comptable. Règle : le
mois du P&L vient de `date_facturation = jm_date + (ec_jour − 1) jours` (période + jour).
Appliqué dans `int_mssql_sage__pnl_bu` et `int_mssql_sage__ecriture_non_ventilee`.

**Signe du montant.** Le staging garde `ea_montant` brut. Règle : débit (`ec_sens = 0`) →
négatif, crédit (`ec_sens = 1`) → positif, **sans `abs()`** : un montant analytique négatif est
une réaffectation entre sections et doit inverser le sens. La somme de
`montant_analytique_signe` donne le résultat net (produits − charges). Appliqué dans
`int_mssql_sage__pnl_bu`.

**Écritures sans ventilation analytique.** Une écriture de classe 6 ou 7 peut n'avoir aucune
ligne dans `f_ecriturea`. Règle : `left join` comptable → analytique, drapeau
`is_missing_analytical` ; ces écritures sont exclues du P&L par BU et listées à part dans
`fct_finance__ecriture_non_ventilee` (bornée par la variable `ecriture_non_ventilee_floor`).
Cette liste exclut le journal de situation `SIT` : ses écritures de clôture sont extournées le
mois suivant et la comptabilité ne les ventile pas. Le P&L, lui, les garde.

**BU non résolue.** La BU d'une section se lit d'abord dans le seed
`ref_mssql_sage__code_analytique_bu`, puis, en repli, par préfixe de section ; une réécriture
historique (source `historic`) corrige des écritures rectifiées hors Sage. Une section absente
du seed et sans préfixe connu n'a pas de BU. Règle : drapeau `is_missing_bu_mapping` dans
l'intermediate, BU `BU_NON_RENSEIGNEE` dans `fct_finance__pnl_bu`. Corriger en ajoutant la
section au seed, pas au `case` de préfixes. Appliqué dans `int_mssql_sage__pnl_bu`.

**Section `ZSITUATION`.** Elle regroupe les écritures de clôture et de régularisation (CCA, FNP,
produits à recevoir, extournes, refacturations internes). Une écriture et son extourne tombent
sur deux mois consécutifs : un mois isolé est faussé, le cumul non. Elle est conservée comme
une BU distincte dans le P&L.

**Budget absent ≠ budget nul.** Le budget vient d'un seed saisi à la main
(`ref_mssql_sage__pnl_budget`). Une BU ou une année absente du seed a un budget `NULL`, pas 0.
Appliqué dans `fct_finance__pnl_bu`.

**`f_docligne` mélange trois domaines.** `do_domaine` : 0 = vente, 1 = achat, 2 = stock. En
domaine stock, `ct_num` est un **numéro de dépôt**, pas un tiers. Règle : toujours filtrer le
domaine avant de joindre `f_comptet` ; le test `relationships` du staging est restreint à
`do_domaine in (0, 1)`.

**`f_comptet` n'est pas une liste de clients.** Elle porte tous les tiers du dossier, clients
(`ct_type = 0`) et fournisseurs (`ct_type = 1`) compris. Règle : filtrer sur `ct_type` pour une
liste de clients ; joindre par le domaine du document (vente → client, achat → fournisseur). La
jointure `f_ecriturec.ct_num → f_comptet` est testée en staging mais n'est utilisée par aucun
modèle : la valider avant de s'en servir.

**Valeurs 0 sans parent.** `co_no = 0` (aucun commercial assigné) est absent de
`f_collaborateur` ; `de_no = 0` (ligne sans mouvement de stock) est absent de `f_depot`.
Règle : `left join`, jamais `inner join` ; les tests `relationships` sur `co_no` et `de_no`
excluent la valeur 0.

**CA doublé par les kits.** Un kit facturé porte aussi les lignes de ses composants. Règle :
ne compter que les lignes valorisées, `dl_valorise = 1`. Appliqué dans
`app_cockpit__nunshen_vente_mensuelle` et `app_cockpit__nunshen_bl_client`.

**Documents transformés.** En `replace`, une commande fournisseur réceptionnée ou une
préparation de fabrication transformée en bon disparaît de `f_docentete` et de `f_docligne`.
Règle : les types 12 (commande fournisseur) et 24 (préparation de fabrication) ne contiennent
que les documents **ouverts** ; leur quantité est déjà le reste à livrer ou à produire. Aucun
historique des commandes n'est conservé. Lire `app_cockpit__nunshen_commande_fournisseur` et
`app_cockpit__nunshen_ordre_production`.

**Photos de stock multiples.** Un run manuel ajoute une seconde photo le même jour. Règle : ne
jamais sommer sur plusieurs photos ; choisir une photo explicitement. Stock : dernière
extraction de chaque jour en heure de Paris (`app_cockpit__nunshen_stock_photo`). Lots : photo
la plus récente (`app_cockpit__nunshen_stock_lot`, `app_cockpit__nunshen_reception`).

Les autres règles du P&L (périmètre des classes 6 et 7, catégorie d'un compte, scénarios) sont
dans `int_mssql_sage__pnl_bu`, `fct_finance__pnl_bu` et leurs descriptions YAML.

---

## Hors lignage dbt

Le job d'export de la masse salariale (`infra/cloudrun_export.tf`) lit
`prod_staging.stg_mssql_sage__f_ecriturec` (`jo_num`, `jm_date`) pour vérifier que la paie du
mois est comptabilisée. Renommer ou retyper ces colonnes le casse sans qu'aucun test dbt ne le voie.

---

## Consommateurs

```bash
dbt ls -s source:mssql_sage+ --resource-type model
```
