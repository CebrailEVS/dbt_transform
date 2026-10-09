# Architecture — MSSQL Sage (`mssql_sage`)

| | |
|---|---|
| Source dbt | `mssql_sage` — `models/staging/mssql_sage/_mssql_sage__sources.yml` |
| Pipeline dlt | `ingestion/pipelines/mssql_sage` — SQL Server, base `EVS_PRO`, schéma `dbo` ; périmètre dans `tables.py` |
| Tables raw | `prod_raw.dbo_f_*`, 15 tables, colonnes plates normalisées en snake_case |
| Fraîcheur | [`docs/freshness.md`](../freshness.md) |
| Cadence | [`docs/pipeline-schedule.md`](../pipeline-schedule.md) ; le workflow enchaîne l'extraction et `dbt build -s source:mssql_sage+` |

**Modes de chargement** (`tables.py`) :

| Mode | Tables | Conséquence en aval |
|---|---|---|
| `replace` (snapshot complet) | faits `f_ecriturec`, `f_ecriturea`, `f_docligne` ; référentiels `f_comptet`, `f_collaborateur`, `f_compteg`, `f_comptea`, `f_article`, `f_docentete`, `f_nomenclat`, `f_depot`, `f_famille`, `f_artfourniss` | le raw ne contient que ce qui existe dans Sage au dernier run : une suppression ou une transformation de document y disparaît |
| `append` (une photo par run) | `f_artstock`, `f_lotserie` | Sage n'a que l'état courant ; chaque run ajoute une photo complète, identifiée par `_extracted_at`. Le staging garde toutes les photos ; le choix d'une photo par jour se fait en aval |

Le dossier `EVS_PRO` porte deux domaines qui ne partagent que la base :

- **Comptabilité, toutes BU** : écritures générales et analytiques, plan comptable, sections
  analytiques. Alimente le P&L par BU (`models/marts/finance/`).
- **Gestion commerciale et stock, BU Nunshen seule** : documents, articles, stock, lots,
  nomenclatures, tiers. Alimente l'application Cockpit Supply (`app_cockpit__nunshen_*`).

---

## Grain et clés

| Table | Rôle | Grain / clé |
|---|---|---|
| `f_ecriturec` | Journal comptable | `ec_no` |
| `f_ecriturea` | Ventilation analytique d'une écriture (0 à N lignes par `ec_no`) | `(ec_no, n_analytique, ea_ligne)` |
| `f_compteg` | Plan comptable général, intitulé des comptes | `cg_num` |
| `f_comptea` | Sections analytiques ; un seul plan analytique | `ca_num` |
| `f_comptet` | Tiers du dossier : clients **et** fournisseurs (`ct_type`, 1 = fournisseur) | `ct_num` |
| `f_collaborateur` | Collaborateurs (représentants commerciaux) | `co_no` |
| `f_docentete` | En-têtes de documents : ventes, achats, stock, fabrication | `(do_type, do_piece)` |
| `f_docligne` | Lignes de documents, mêmes domaines | `dl_no` |
| `f_article` | Référentiel articles, champs libres Nunshen compris | `ar_ref` |
| `f_famille` | Familles d'articles ; toutes de détail (`fa_type = 0`) | `fa_code_famille` |
| `f_nomenclat` | Nomenclatures : lien composé → composant, avec quantité | `(ar_ref, no_ref_det)` en pratique |
| `f_artfourniss` | Référence et tarif d'achat par article × fournisseur ; un fournisseur principal (`af_principal = 1`) | `(ar_ref, ct_num)` |
| `f_depot` | Dépôts de stock | `de_no` |
| `f_artstock` | Stock et valeur (CMUP) par article × dépôt, une photo par run | `extracted_at × ar_ref × de_no` |
| `f_lotserie` | Lots et numéros de série : une ligne par mouvement de lot (entrée `dl_no_in`, sortie `dl_no_out`, 0 tant que le lot n'est pas sorti), une photo par run | aucune clé unique déclarée par Sage |

`cb_marq` est l'identifiant technique Sage de chaque ligne. Sur les tables en `append`, il
n'est unique **qu'à l'intérieur d'une photo**.

---

## Pièges

**Écriture en double après une modification dans Sage.** Sage crée parfois un nouveau
`cb_marq` lors d'une mise à jour au lieu de modifier la ligne. Règle : dédoublonner sur la clé
métier en gardant le `cb_marq` le plus récent. Appliqué dans `stg_mssql_sage__f_ecriturec`
(`ec_no`) et `stg_mssql_sage__f_ecriturea` (`ec_no, n_analytique, ea_ligne`) ; les référentiels
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

**BU non résolue.** Une section absente du seed et sans préfixe connu n'a pas de BU. Règle :
drapeau `is_missing_bu_mapping` dans l'intermediate, BU `BU_NON_RENSEIGNEE` dans
`fct_finance__pnl_bu`. Corriger en ajoutant la section au seed, pas au `case` de préfixes.

**Section `ZSITUATION`.** Elle regroupe les écritures de clôture et de régularisation (CCA, FNP,
produits à recevoir, extournes, refacturations internes). Une écriture et son extourne tombent
sur deux mois consécutifs : un mois isolé est faussé, le cumul non. Elle est conservée comme
une BU distincte dans le P&L.

**`f_docligne` mélange trois domaines.** `do_domaine` : 0 = vente, 1 = achat, 2 = stock. En
domaine stock, `ct_num` est un **numéro de dépôt**, pas un tiers. Règle : toujours filtrer le
domaine avant de joindre `f_comptet` ; le test `relationships` du staging est restreint à
`do_domaine in (0, 1)`.

**`f_comptet` n'est pas une liste de clients.** Elle porte les clients et les fournisseurs du
dossier. Règle : filtrer sur `ct_type` pour une liste de clients ; joindre par le domaine du
document (vente → client, achat → fournisseur). La jointure `f_ecriturec.ct_num → f_comptet`
n'est ni utilisée ni testée : la valider avant de s'en servir.

**`co_no = 0`.** Valeur Sage « aucun commercial assigné », absente de `f_collaborateur`.
Règle : `left join` vers `f_collaborateur`, jamais `inner join`.

**Dépôt 0.** Les lignes de document sans mouvement de stock portent `de_no = 0`, absent de
`f_depot`. Règle : `left join` ; le test `relationships` exclut `de_no = 0`.

**CA doublé par les kits.** Un kit facturé porte aussi les lignes de ses composants. Règle :
ne compter que les lignes valorisées, `dl_valorise = 1`. Appliqué dans
`app_cockpit__nunshen_vente_mensuelle` et `app_cockpit__nunshen_bl_client`.

**Documents transformés.** En `replace`, une commande fournisseur réceptionnée ou une
préparation de fabrication transformée en bon disparaît de `f_docentete` et de `f_docligne`.
Règle : les types 12 (commande fournisseur) et 24 (préparation de fabrication) ne contiennent
que les documents **ouverts** ; leur quantité est déjà le reste à livrer ou à produire. Aucun
historique des commandes n'est conservé. Lire `app_cockpit__nunshen_commande_fournisseur` et
`app_cockpit__nunshen_ordre_production`. Joindre lignes et en-têtes sur `(do_type, do_piece)` :
`do_piece` seul n'est pas unique.

**Photos de stock multiples.** Un run manuel ajoute une seconde photo le même jour. Règle : ne
jamais sommer sur plusieurs photos ; choisir une photo explicitement. Stock : dernière
extraction de chaque jour en heure de Paris (`app_cockpit__nunshen_stock_photo`). Lots : photo
la plus récente (`app_cockpit__nunshen_stock_lot`, `app_cockpit__nunshen_reception`).

---

## Règles métier et leur source

| Règle | Source | Appliquée dans |
|---|---|---|
| Périmètre du P&L : comptes généraux de classes 6 (charges) et 7 (produits) | plan comptable | `int_mssql_sage__pnl_bu` |
| Catégorie P&L d'un compte (CA, MP & SSTT, masse salariale, frais directs & amortissements) | seed `ref_mssql_sage__code_comptable_bu` | `int_mssql_sage__pnl_bu` |
| BU d'une section, par cascade : 1. seed ; 2. préfixe (`NUN` → NUNSHEN ; `HOR`, `OFF`, `COM` → COMMERCE ; `NES` → NESHU ; `SAV` → TECHNIQUE ; `PDET` → PIECES DET) ; 3. réécriture des écritures 2024 | seed `ref_mssql_sage__code_analytique_bu`, source `historic.update_mssql_sage__analytique_2024` (rectifications faites hors Sage) | `int_mssql_sage__pnl_bu` |
| Scénario `SANS_PROVISIONS_CP` : exclut les comptes 645800 et 641200 (provisions congés payés) | finance | `fct_finance__pnl_bu` |
| Budget mensuel par BU et catégorie ; une BU ou une année absente du seed a un budget `NULL`, pas 0 | seed `ref_mssql_sage__pnl_budget`, saisi à la main | `fct_finance__pnl_bu` |
| CA Nunshen : factures (`do_type` 6 et 7) du domaine vente, lignes valorisées ; avoirs déjà signés | Sage | `app_cockpit__nunshen_vente_mensuelle` |
| Stock disponible : dépôt NUNSHEN (`de_no = 1`) ; valeur de stock : tous dépôts, CMUP = `as_mont_sto / as_qte_sto` | métier Nunshen | `app_cockpit__nunshen_stock_photo` |
| Lot en stock : ligne d'entrée (`ls_mvt_stock = 1`) avec quantité restante > 0 | Sage | `app_cockpit__nunshen_stock_lot` |
| Article actif : `ar_sommeil = 0` ; champs libres `OUI`/`NON` typés en booléens en aval, pas en staging | Sage | `app_cockpit__nunshen_article` |

---

## Consommateurs

Deux familles : le P&L de `models/marts/finance/` et les tables Nunshen de l'application
Cockpit Supply (`models/apps/cockpit_supply/app_cockpit__nunshen_*`). Liste à jour :

```bash
dbt ls -s source:mssql_sage+ --resource-type model
```
