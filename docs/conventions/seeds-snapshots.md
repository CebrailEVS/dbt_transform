# Conventions — Seeds & Snapshots

> Ces deux types de ressources ne sont pas des couches du DAG mais ont leurs
> propres règles. Transversal : [`../../CONVENTIONS.md`](../../CONVENTIONS.md).

## Seeds (`data/reference_data/`)

Fichiers CSV chargés via `dbt seed`. Données de référence statiques (mappings,
paramètres). Organisés par domaine source, **un fichier YAML de doc par source**
co-localisé avec ses CSV (comme staging/marts) :

```
data/reference_data/
├── yuman/              # ref_yuman__*.csv          + _yuman__seeds.yml
├── oracle_neshu/       # ref_oracle_neshu__*.csv   + _oracle_neshu__seeds.yml
├── oracle_lcdp/        # ref_oracle_lcdp__*.csv     + _oracle_lcdp__seeds.yml
├── mssql_sage/         # ref_mssql_sage__*.csv     + _mssql_sage__seeds.yml
├── nesp_tech/          # ref_nesp_tech__*.csv      + _nesp_tech__seeds.yml
├── nesp_co/            # ref_nesp_co__*.csv        + _nesp_co__seeds.yml
├── zoho_desk/          # ref_zoho_desk__*.csv      (doc YAML à créer)
└── general/            # ref_general__*.csv        + _general__seeds.yml
```

- Nommage CSV : `ref_<source>__<entity>.csv`. Schéma de destination : `prod_reference` / `dev_reference`.
- Nommage YAML : `_<source>__seeds.yml`, **un par source**, dans le dossier de la source.
  `seed-paths: ["data"]` → dbt découvre ces YAML automatiquement (pas de config à toucher).
  Il n'y a **plus** de `data/schema.yml` monolithique.

### Déclaration des types de colonnes

Types déclarés **dans `_<source>__seeds.yml`** sous chaque seed, via `config: column_types:`.
Ne pas utiliser `dbt_project.yml` pour les types par colonne.

```yaml
- name: ref_nesp_tech__key_facturation
  description: "Cle de facturation des interventions techniques"
  config:
    column_types:
      type_code: STRING
      prod_factu: INT64
      tarif_factu: FLOAT64
      valid_from: DATE
  columns:
    - name: type_code
      ...
```

### Types BigQuery à utiliser

| Type | Utiliser | Ne pas utiliser |
|------|----------|-----------------|
| Texte | `STRING` | `string`, `str`, `VARCHAR` |
| Entier | `INT64` | `int`, `integer`, `INTEGER` |
| Décimal | `FLOAT64` | `float`, `FLOAT`, `NUMERIC` |
| Date | `DATE` | `date` |
| Timestamp | `TIMESTAMP` | `timestamp`, `DATETIME` |
| Booléen | `BOOLEAN` | `bool`, `BOOL` |

> **Règle critique** : toujours vérifier les valeurs réelles du CSV avant
> d'assigner un type. Un code postal sans zéro initial (`38000`) sera inféré
> `INT64` par BigQuery — le typer `STRING` casserait les joins existants. Ne pas
> se fier au nom de la colonne seul.

### Colonnes dans `_<source>__seeds.yml`

Chaque colonne du CSV doit avoir une entrée dans `columns:` avec une description.
Les colonnes importantes doivent avoir des tests (`not_null`, `unique`, `relationships`).

### BOM dans les fichiers CSV

Ne pas sauvegarder les CSV depuis Excel en UTF-8 avec BOM. Si un fichier contient
un BOM (visible via `head -c 3 fichier.csv | xxd`), le supprimer avec :

```bash
sed -i '1s/^\xef\xbb\xbf//' data/reference_data/<source>/<fichier>.csv
```

## Snapshots (`snapshots/`)

Suivi historique SCD Type 2 sur les entités qui évoluent :

| Snapshot | Entité | Stratégie |
|----------|--------|-----------|
| `snap_oracle_neshu__company` | Clients | timestamp |
| `snap_oracle_neshu__device` | Machines | timestamp |
| `snap_oracle_neshu__valo_parc_machines` | Parc machines | timestamp |

Commande : `dbt snapshot`.

> **Hard rule** : stratégie/colonnes des snapshots inchangées (gérés par GCP Cloud
> Workflows). Ne jamais renommer le fichier snapshot ni sa table BQ (historique
> SCD2 perdu). **Exception** : mettre à jour les `ref()` internes quand une dim
> référencée est renommée est OK.
