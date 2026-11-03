# Architecture — Yuman stock théorique (SFTP)

| | |
|---|---|
| Source dbt | `yuman_evs_sftp` (`models/staging/yuman_evs_sftp/_yuman_evs_sftp__sources.yml`) |
| Pipeline | `ingestion/pipelines/yuman_evs_stock` : un CSV déposé sur le SFTP du fournisseur |
| Chargement | `merge` / `delete-insert` sur `export_date`, table partitionnée sur cette colonne : un jour rejoué écrase sa partition, les jours s'accumulent |

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).

Source **complémentaire** à [`yuman.md`](yuman.md) : le fichier (`;`, latin-1, champs quotés)
porte le **stock théorique des entrepôts Yuman**, que l'API n'expose pas. Le pipeline archive
chaque CSV **brut** avant de le lire, à un chemin déterministe par `export_date` (`ARCHIVE` dans
`tables.py`) : c'est la seule trace du fichier tel que le fournisseur l'écrit.

---

## Clés et grain

- Grain : 1 ligne par (article × emplacement × jour d'export), mais ce triplet **n'est pas
  unique** : le fichier contient de vrais doublons. La clé de ligne est `_dlt_id`, générée par
  dlt ; le test de grain porte dessus.
- `export_date` est la date de **modification du fichier** sur le SFTP, pas la date du run.
- Un emplacement dont le nom contient `DEPOT` est un atelier physique ; tout autre est le stock
  embarqué d'un technicien (équivalent des `storehouses` de `yuman.md`). Règle portée par la
  macro `yuman_stock_type` (`macros/yuman/yuman_stock_type.sql`).

---

## Pièges

### Noms de colonnes brutes mutilés par l'encodage
- **Symptôme** : `r_f_rence`, `d_signation`, `quantitx` dans le raw.
- **Règle** : le normaliseur dlt remplace les caractères accentués ; l'accent final de `Quantité`
  donne `quantitx`, pas `quantit_`. Toujours partir du staging.
- **Appliqué par** : `stg_yuman_evs_sftp__stock_theorique`.

### Doublons réels dans la source
- **Symptôme** : un test `unique` sur `(export_date, reference, nom_du_stock)` échoue.
- **Règle** : les doublons du fichier sont conservés tels quels et comptés deux fois dans les
  sommes des marts. Ne pas déclarer ce triplet unique ; le test porte sur `_dlt_id`. Un jour
  rejoué ne peut pas s'empiler : le merge sur `export_date` l'écrase.
- **Appliqué par** : `stg_yuman_evs_sftp__stock_theorique`.

### Virgule décimale française
- **Symptôme** : erreur de cast à la construction du staging.
- **Règle** : les quantités arrivent en `"1,5"`. Le cast reste dans dbt (le raw est fidèle à la
  source), sans `safe_cast` : une valeur non castable fait échouer le build, ce qui est voulu.
- **Appliqué par** : `stg_yuman_evs_sftp__stock_theorique`.

### `nom_du_stock IS NULL` ⇔ `quantite = 0`
- **Symptôme** : une part importante des lignes n'a pas d'entrepôt.
- **Règle** : c'est une règle de l'export Yuman, pas une perte à l'extraction : une référence
  sans stock physique n'est rattachée à aucun entrepôt. Au niveau brut, la valeur est une chaîne
  vide, convertie en `NULL` par le staging. Aucun ticket à ouvrir côté fournisseur.
- **Conséquence** : le staging garde ces lignes ; `fct_supply_chain__stock_yuman` les filtre
  (`nom_du_stock is not null`) et ne contient que les positions réelles. Le catalogue complet
  des références n'est disponible qu'au staging.
- **Appliqué par** : `fct_supply_chain__stock_yuman`.

### Source non rétroactive
- **Symptôme** : un jour absent de `export_date`.
- **Règle** : le fournisseur n'expose que le fichier courant. Un jour manqué est perdu, sans
  reprise possible. Le test de fraîcheur est la seule détection d'un jour perdu ; il couvre aussi
  un fichier que le fournisseur cesse de rafraîchir (`max(export_date)` n'avance plus).
- **Ne pas** interpréter un trou de `export_date` comme un stock nul.

---

## Règles de jointure inter-sources

- **Stock → catalogue Yuman** : pas de `product_id`, seulement `reference` (texte). Joindre
  `reference = stg_yuman__products.product_code` : jointure textuelle, exposée aux typos, espaces
  et différences de casse.

---

## Consommateurs

```bash
dbt ls -s source:yuman_evs_sftp+
```
