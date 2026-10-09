# Architecture — Yuman stock théorique (SFTP)

| | |
|---|---|
| Source dbt | `yuman_evs_sftp` (`models/staging/yuman_evs_sftp/_yuman_evs_sftp__sources.yml`) |
| Pipeline dlt | `yuman_evs_stock` (`ingestion/pipelines/yuman_evs_stock`) |
| Table raw | `prod_raw.sftp_yuman_evs_stock_theorique`, partitionnée sur `export_date` |
| Chargement | `merge` / `delete-insert` sur `export_date` : un jour rejoué écrase sa partition |

Fraîcheur : `docs/freshness.md`. Cadence et orchestration : `docs/pipeline-schedule.md`.

Source **complémentaire** à `docs/architecture/yuman.md` : le fournisseur dépose
un CSV (`;`, latin-1, champs quotés) sur son SFTP, avec le **stock théorique des
entrepôts Yuman**, que l'API n'expose pas.

Chaîne : SFTP → `prod_raw` → `stg_yuman_evs_sftp__stock_theorique` → marts
`supply_chain/`. Pas de couche intermediate.

Le pipeline archive chaque CSV **brut** avant de le lire (`ARCHIVE` dans
`tables.py`, `archiver()` dans `source.py`), à un chemin déterministe par
`export_date`. C'est la seule trace du fichier tel que le fournisseur l'écrit.
L'ancienne archive JSONL (parsée par Singer) est figée et ne contient pas la
source brute ; dbt ne la lit pas.

---

## Grain et clés

Grain : **1 ligne par (article x emplacement x jour d'export)**, mais ce triplet
n'est **pas unique** : le fichier du fournisseur contient de vrais doublons.
La clé de ligne est `_dlt_id`, générée par dlt ; le test de grain porte dessus.

| Colonne | Règle |
|---|---|
| `reference`, `designation` | `trim` des colonnes brutes `r_f_rence`, `d_signation` |
| `quantite` | `cast(replace(quantitx, ',', '.') as float64)` |
| `nom_du_stock` | `nullif(trim(...), '')` |
| `export_date` | date de **modification du fichier** sur le SFTP, pas la date du run |
| `_dlt_id` | seule clé de ligne disponible |

Les stocks `ST - NOM PRENOM` sont les stocks embarqués des techniciens
(équivalent des `storehouses` de `yuman.md`) ; les `XX - DEPOT` sont les
ateliers physiques.

---

## Pièges

### Noms de colonnes brutes mutilés par l'encodage
- **Symptôme** : `r_f_rence`, `d_signation`, `quantitx` dans le raw.
- **Règle** : le normaliseur dlt remplace les caractères accentués ; l'accent
  final de `Quantité` donne `quantitx`, pas `quantit_`. Toujours partir du staging.
- **Appliqué par** : `stg_yuman_evs_sftp__stock_theorique`.

### Doublons réels dans la source
- **Symptôme** : un test `unique` sur `(export_date, reference, nom_du_stock)` échoue.
- **Règle** : le fichier contient de vrais doublons, conservés tels quels. Ils
  sont comptés deux fois dans les sommes des marts. Ne pas déclarer ce triplet
  unique ; le test porte sur `_dlt_id`. Un jour rejoué ne peut pas s'empiler :
  le merge sur `export_date` l'écrase.
- **Appliqué par** : `stg_yuman_evs_sftp__stock_theorique`.

### Virgule décimale française
- **Symptôme** : erreur de cast à la construction du staging.
- **Règle** : les quantités arrivent en `"1,5"`. Le cast reste dans dbt (le raw
  est fidèle à la source). Il n'y a pas de `safe_cast` : une valeur non
  castable fait échouer le build, ce qui est voulu.
- **Appliqué par** : `stg_yuman_evs_sftp__stock_theorique`.

### `nom_du_stock IS NULL` ⇔ `quantite = 0`
- **Symptôme** : une part importante des lignes n'a pas d'entrepôt.
- **Règle** : c'est une règle de l'export Yuman, pas une perte à l'extraction :
  une référence sans stock physique n'est rattachée à aucun entrepôt. Au niveau
  brut, la valeur est une chaîne vide, convertie en `NULL` par le staging.
  Aucun ticket à ouvrir côté fournisseur.
- **Conséquence** : le staging garde ces lignes ; `fct_supply_chain__stock_yuman`
  les filtre (`nom_du_stock is not null`) et ne contient donc que les positions
  réelles. Le catalogue complet des références n'est disponible qu'au staging.
- **Appliqué par** : `fct_supply_chain__stock_yuman`.

### Source non rétroactive
- **Symptôme** : un jour absent de `export_date`.
- **Règle** : le fournisseur n'expose que le fichier courant. Un jour manqué
  est perdu, sans reprise possible. Le test de fraîcheur est la seule détection
  d'un jour perdu ; il couvre aussi un fichier que le fournisseur cesse de
  rafraîchir (`max(export_date)` n'avance plus).
- **Ne pas** interpréter un trou de `export_date` comme un stock nul.

### Pas de clé vers le catalogue Yuman
- **Symptôme** : impossible de relier le stock à un produit par id.
- **Règle** : la table porte `reference` (texte), pas `product_id`. Joindre
  `reference = stg_yuman__products.product_code` : jointure textuelle, exposée
  aux typos, espaces et différences de casse.

---

## Règles métier

- Le stock théorique est la photo du jour du fournisseur : `export_date` suit le
  `mtime` du fichier, pas l'heure du run.
- Le type de stock (dépôt ou stock embarqué) est dérivé du nom par la macro
  `yuman_stock_type`.
- La rupture par dépôt est reconstruite dans
  `fct_supply_chain__rupture_depot_yuman` à partir de ce stock, des techniciens
  et des consommations d'articles Yuman.

---

## Consommateurs

```bash
dbt ls -s source:yuman_evs_sftp+
```
