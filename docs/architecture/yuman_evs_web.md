# Architecture — Yuman journal des mouvements de stock (web)

| | |
|---|---|
| Source dbt | `yuman_evs_web` (`models/staging/yuman_evs_web/_yuman_evs_web__sources.yml`) |
| Pipeline | `ingestion/pipelines/yuman_evs_mouvements_stock` : export CSV de l'écran Achats/Stocks › Articles › Journal, déclenché par un navigateur automatisé |
| Chargement | fenêtre glissante des 7 dernières journées, `merge` / `delete-insert` sur `journee` (table partitionnée sur cette colonne) : une journée rechargée remplace la précédente |

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).

Source **complémentaire** à [`yuman.md`](yuman.md) et [`yuman_evs_sftp.md`](yuman_evs_sftp.md) :
ni l'API ni le fichier SFTP n'exposent le journal des mouvements. Le nom `yuman_evs_web` est
imposé par le workflow (`source:yuman_evs_web+`) : ne pas le renommer.

---

## Clés et grain

- Grain : 1 ligne = 1 mouvement de stock. Le journal n'est jamais réécrit : une erreur se
  corrige par un contre-mouvement (observation « Erreur double transmission », par exemple).
- **Pas de clé naturelle** : des lignes identiques (même minute, même article, mêmes stocks) sont
  de vrais mouvements, conservés. La clé de ligne est `_dlt_id`.
- Sens du mouvement, lu dans le couple de stocks : départ seul = sortie, destination seule =
  entrée, les deux = transfert ou consommation. `quantite` est toujours positive.
- Libellés de stock : `ST - <technicien>`, `NN - <dépôt>`, `Bon de commande - <n°>` (réception
  fournisseur), `Intervention - <n°>` (consommation sur intervention).
- `quantite_stock_de_depart` / `_destination` : niveau du stock **après** le mouvement.
- **Ordre des mouvements** : `operation_at`, puis `_ligne` à l'intérieur d'une journée. L'instant
  est à la minute ; `_ligne` est le rang de la ligne dans son export, en ordre chronologique.

---

## Pièges

### `_dlt_id` unique mais pas stable
- **Symptôme** : un `_dlt_id` d'hier est introuvable aujourd'hui.
- **Règle** : dlt le régénère à chaque rechargement, donc chaque nuit sur les 7 dernières
  journées. Clé de ligne du staging, jamais clé persistante (snapshot, incrémental en aval).
- **Appliqué par** : `stg_yuman_evs_web__mouvements_stock`.

### Pas d'incrémental en ajout
- **Symptôme** : les 7 dernières journées en double dans un modèle incrémental.
- **Règle** : chaque run réécrit la fenêtre avec un nouvel `_extracted_at` ; un ajout filtré sur
  `extracted_at` la doublerait. Le motif « table d'historique sans clé : l'ajout seul » de
  [`staging.md` § 5](../conventions/staging.md#5-matérialisation) ne s'applique pas : `table`.
- **Appliqué par** : `stg_yuman_evs_web__mouvements_stock`.

### La journée J n'est complète qu'au run de J+1
- **Symptôme** : la dernière journée paraît incomplète.
- **Règle** : des automatismes Yuman écrivent vers 23:00 et après minuit, après le run de 22:00.
  Le run du lendemain recharge la journée. Une ligne horodatée 00:xx appartient à la journée J+1.

### Automatismes nocturnes
- **Symptôme** : des sorties `ST - …` sans destination ni utilisateur, entre 23:00 et 00:10.
- **Règle** : ce sont des décréments automatiques. Leur observation porte une **date antérieure
  de plusieurs jours** au mouvement, sans doute la date réelle de consommation dans un autre
  système. **À trancher avant un mart** : quelle date fait foi.
- **Ne pas** additionner ces décréments avec `nesp_tech` sans rapprochement : le flux sortant
  `export-nesp-tech-stock-yuman` envoie `stg_nesp_tech__articles` vers Yuman, et ces décréments
  ou les « Import de données » en proviennent peut-être.

### Noms de colonnes brutes mutilés, tout en texte
- **Symptôme** : `qtx`, `r_ference`, `num_ro_de_s_rie` dans le raw ; virgule décimale.
- **Règle** : le normaliseur dlt mutile les accents et l'ingestion ne type rien. Le staging
  renomme d'après l'en-tête du CSV et caste sans `safe_cast` : une valeur mal formée fait échouer
  le build, ce qui est voulu.
- **Appliqué par** : `stg_yuman_evs_web__mouvements_stock`.

### `_ligne` ne vaut qu'à l'intérieur d'une journée
- **Symptôme** : le `_ligne` d'un mouvement d'hier a changé aujourd'hui.
- **Règle** : c'est le rang dans le fichier exporté, et la fenêtre d'export glisse chaque nuit.
  Seul l'ordre à l'intérieur d'une journée est stable : pour une clé de mouvement, calculer le
  rang dans la journée. Une journée vient d'un seul export.
- **Appliqué par** : `app_cockpit__technique_mouvement_stock` (`rang_journee`) ; test
  `assert_stg_yuman_evs_web__une_journee_un_export`.

---

## Règles de jointure

- **Article** : `reference = stg_yuman__products.product_code`. L'API Yuman est chargée la nuit
  en semaine, le journal le soir : un article créé et déplacé le jour même n'est dans
  `stg_yuman__products` qu'au chargement suivant.
- **Utilisateur** : `utilisateur = stg_yuman__users.user_name` (le nom seul, pas d'identifiant).
  `Import de données` n'est pas un utilisateur.
- **Stocks** : libellé = `stg_yuman__storehouses.storehouses_name` pour `ST` et `NN`. Des stocks
  renommés ou supprimés dans Yuman restent introuvables.
- **Intervention et bon de commande** : le numéro s'extrait du libellé de stock en intermediate
  (`workorder_number`, `purchase_order_number`). Quelques libellés d'intervention ont un format
  `N / 9 / 9` qui ne se joint pas.

---

## Consommateurs

```bash
dbt ls -s source:yuman_evs_web+
```
