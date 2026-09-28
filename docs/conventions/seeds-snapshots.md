# Conventions — Seeds & snapshots

Règles transversales : [CONVENTIONS.md](../../CONVENTIONS.md).

## Seeds

Les seeds sont des CSV de référence statiques (mappings, paramètres), chargés par `dbt seed` dans
`prod_reference`, ou dans `dbt_<dev>` en dev.

```
data/reference_data/<source>/
├── ref_<source>__<entite>.csv
└── _<source>__seeds.yml        # doc + tests + column_types, un fichier par source
```

Sources actuelles : `general`, `mssql_sage`, `nesp_co`, `nesp_tech`, `oracle_lcdp`,
`oracle_neshu`, `yuman`, `zoho_desk`.

> Dette connue : `zoho_desk` n'a pas encore son `_zoho_desk__seeds.yml`.

### Types des colonnes

On les déclare **dans `_<source>__seeds.yml`**, avec `config: column_types`, pour **toutes** les
colonnes. Jamais dans `dbt_project.yml`.

```yaml
- name: ref_<source>__<entite>
  description: "Ce que contient la table, et son grain."
  config:
    column_types:
      code: STRING
      libelle: STRING
      tarif: FLOAT64
      valid_from: DATE
  columns:
    - name: code
      description: "Code métier."
      tests: [not_null]
```

Types à utiliser : `STRING`, `INT64`, `FLOAT64`, `DATE`, `TIMESTAMP`, `BOOLEAN`.

**Choisir le type d'après les valeurs réelles, pas d'après le nom de la colonne.** Un code
postal `38000` inféré en `INT64` et retypé en `STRING` casse les jointures existantes :
`head -3 <fichier>.csv` avant de décider.

### Encodage

UTF-8 **sans BOM**. Ne pas enregistrer depuis Excel. Pour retirer un BOM :

```bash
sed -i '1s/^\xef\xbb\xbf//' data/reference_data/<source>/<fichier>.csv
```

Tester : `dbt build -s ref_<source>__<entite>+` (le seed et tout son aval).

## Snapshots

Les snapshots gardent l'historique **SCD2** des entités qui évoluent. Ils sont construits
**uniquement en prod**, par un workflow Cloud Workflows dédié (`dbt snapshot`), et sont exclus
de la CI/CD et des builds de dev. Le dev les lit en prod grâce au defer.

| Snapshot | Construit à partir de | Stratégie |
|---|---|---|
| `snap_oracle_neshu__company` | `dim_neshu__company` | `check` |
| `snap_oracle_neshu__device` | `dim_neshu__device` | `check` |
| `snap_oracle_neshu__valo_parc_machines` | `int_oracle_neshu__valorisation_parc_machines` | `timestamp` |
| `snap_lcdp__device` | `dim_lcdp__device` | `check` |
| `snap_yuman__storehouses` | `stg_yuman__storehouses` | `check` |
| `snap_yuman__users` | `stg_yuman__users` | `check` |

Ils sont exposés en marts sous forme de dimension `_history` (cf. [marts.md § 3](marts.md#3-modélisation--schéma-en-étoile-strict)).

**Règles strictes**
- Ne jamais changer la stratégie ni les colonnes suivies d'un snapshot.
- Ne jamais renommer le fichier ni la table BigQuery : l'historique SCD2 serait perdu.
- Seule exception autorisée : mettre à jour un `ref()` interne quand la dimension source est renommée.
