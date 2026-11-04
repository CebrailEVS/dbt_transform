# Conventions

Index des règles du projet. Ce fichier ne contient que ce qui est **commun à toutes les couches** ;
le détail vit dans une page par couche.

| Tu écris… | Référence |
|---|---|
| un staging `stg_*` | [docs/conventions/staging.md](docs/conventions/staging.md) |
| un intermediate `int_*` | [docs/conventions/intermediate.md](docs/conventions/intermediate.md) |
| un mart `dim_*` / `fct_*` | [docs/conventions/marts.md](docs/conventions/marts.md) |
| un modèle de service d'application `app_*` | [docs/conventions/apps.md](docs/conventions/apps.md) |
| un seed ou un snapshot | [docs/conventions/seeds-snapshots.md](docs/conventions/seeds-snapshots.md) |
| un test de fraîcheur | [docs/freshness.md](docs/freshness.md) |
| rien, tu veux comprendre dev / CI / prod | [docs/environnements.md](docs/environnements.md) |
| rien, tu veux savoir pourquoi le projet est fait ainsi | [docs/decisions.md](docs/decisions.md) |

---

## Nommage

`<prefixe>_<source ou BU>__<entite>` : le double underscore sépare le périmètre de l'entité.

| Couche | Préfixe | Périmètre | Exemple |
|---|---|---|---|
| Staging | `stg_` | source | `stg_oracle_neshu__company` |
| Intermediate | `int_` | source | `int_oracle_lcdp__appro_tasks` |
| Dimension | `dim_` | **BU** | `dim_neshu__company` |
| Fait | `fct_` | **BU** | `fct_neshu__consommation` |
| Service d'application | `app_` | **application** | `app_cockpit__neshu_stock_photo` |
| Snapshot | `snap_` | source | `snap_oracle_neshu__device` |
| Seed | `ref_` | source | `ref_nesp_tech__key_facturation` |

| Fichier YAML | Contenu |
|---|---|
| `_<source>__sources.yml` | déclaration des sources et fraîcheur |
| `_<source>__models.yml` | doc et tests du staging |
| `_<source>__intermediate_models.yml` | doc et tests de l'intermediate |
| `_<bu>__marts_models.yml` | doc et tests des marts |
| `_<application>__app_models.yml` | doc et tests de la couche `apps` |
| `_<source>__seeds.yml` | doc, tests et `column_types` des seeds |
| `_<source>__snapshots.yml` | doc et tests des snapshots |

### Colonnes

| Règle | Exemple |
|---|---|
| snake_case | `company_name` |
| IDs : nom source en staging, `<entite>_id` en marts | `idcompany` → `company_id` |
| Booléens : `is_` / `has_` | `is_active` |
| Timestamps : `_at` · dates : `_date` | `created_at`, `start_date` |
| Mesures : préfixe de nature | `qty_`, `nb_`, `ca_`, `taux_` ([marts.md § 6](docs/conventions/marts.md#6-nommage-des-mesures)) |

---

## Fuseau horaire

Stocker en UTC, afficher en heure de Paris.

| Couche | Règle |
|---|---|
| Staging | Instants en `TIMESTAMP` UTC. Une source qui écrit l'heure de Paris sans fuseau (ERP Oracle) est convertie à l'entrée par `paris_vers_utc` ; une date calendaire devient une `DATE` ([staging.md](docs/conventions/staging.md#dates-des-erp-oracle)). |
| Intermediate | Les instants restent en UTC. Un jour, une semaine ou un mois métier se calcule en heure de Paris : `date(x, 'Europe/Paris')`, `extract(year from x at time zone 'Europe/Paris')`, `current_date('Europe/Paris')`. |
| Marts, apps | Les instants d'un ERP Oracle sortent en heure de Paris dans un `TIMESTAMP` (`utc_vers_paris_affichage`), que Power BI et l'app Cockpit affichent tels quels. Ce ne sont plus des instants : un mart qui calcule avec une heure lue dans une dim la repasse en UTC (`paris_vers_utc`) avant de la comparer à `current_timestamp()` ou à une autre source. |

`date(x)` sans fuseau sur un instant UTC donne la veille pour un événement entre 00:00 et 02:00 à
Paris : l'erreur ne casse rien et ne se voit pas.

---

## Matérialisation

| Couche | Défaut | Exception |
|---|---|---|
| Staging | `table` | `incremental` (`merge`) : grosses tables de tâches Oracle |
| Intermediate | `table` | `incremental` (`merge`) : modèles à gros volume |
| Marts | `table` | — |
| Apps | explicite par modèle | cas par cas ([apps.md](docs/conventions/apps.md#3-matérialisation)) |

Partition sur la date filtrée (Power BI ou incrémental), cluster sur les FK les plus jointes
(4 au maximum). Pas de partition sur les petites dimensions.

---

## Tests

Les arguments d'un test générique s'écrivent sous `arguments:`, la sévérité sous `config:` :

```yaml
- relationships:
    arguments:
      to: ref('dim_neshu__company')
      field: company_id
    config:
      severity: warn
```

Un test en `error` arrête le build et son aval ; un `warn` signale sans bloquer. Le critère :
**`error` quand la règle est garantie, par une contrainte de la source ou par notre code, et
qu'un échec signale donc un bug de la chaîne ; `warn` quand la donnée peut légitimement l'enfreindre.**

| Sévérité | Pour |
|---|---|
| `error` (défaut) | PK `unique` + `not_null`, clés composites, `not_null` des FK obligatoires, `accepted_values` en marts ; `relationships` garanties : FK déclarée par la source (staging), clé construite par notre code |
| `warn` | `relationships` où un orphelin est possible par nature (suppression non propagée, dimension filtrée, référentiel chargé à un autre rythme : le cas courant en marts), plages de valeurs, volumes, `accepted_values` en staging et intermediate (une nouvelle valeur source ne doit pas bloquer l'aval) |

Minimum par couche : voir la page de chaque couche. Paquets : `dbt_utils`, `dbt_expectations`.

---

## Règles vérifiées par `dbt check`

Les règles d'architecture qu'une requête peut vérifier vivent dans [`checks/`](checks/), une
règle par fichier SQL. Chaque `dbt build` les exécute en dev et en CI, pas en prod. Une règle en
`error` arrête le build et nomme les modèles en cause ; en `warn`, elle liste un stock à résorber,
et passe en `error` une fois ce stock nul. Une exception admise s'ajoute dans la règle, avec sa
raison.

## Lint SQL

`dbt lint`, intégré à dbt v2, applique `.sqlfluff`, qui fait foi : mots-clés, fonctions et types
en minuscules, alias explicites (`as`), indentation de 4 espaces, pas de virgule finale. Il ne se
connecte pas à BigQuery, mais doit résoudre les `env_var()` de `profiles.yml` (`.env` chargé).

```bash
dbt lint models/chemin/            # analyser
dbt lint models/chemin/ --fix      # corriger, puis relire le git diff
dbt lint --changed                 # seulement les fichiers modifiés
```

Exception ponctuelle : `-- noqa: RF02` en fin de ligne.

> Dans `.sqlfluff`, `capitalisation.functions` et `capitalisation.types` prennent
> `extended_capitalisation_policy`. Avec `capitalisation_policy`, la règle est ignorée sans message.

---

## Tags

Posés par dossier dans `dbt_project.yml` (couche + source), **jamais** dans un `config()` :

```bash
dbt build -s tag:oracle_neshu     # toute une source
dbt build -s tag:marts            # toute une couche
```

---

## Écrire : commentaires, descriptions, documentation

Tout texte du dépôt décrit **la règle et sa raison**, pas l'histoire ni l'état du moment.

- **À écrire** : le pourquoi d'une règle non évidente, une règle métier et sa source, un piège qui
  se reproduirait. Un commentaire tient en 1 à 3 lignes.
- **Ce que le code liste déjà, on y renvoie** : tables, modèles, BU, tags, seuils, horaires vivent
  dans les fichiers qui les portent (`dbt ls -s source:<x>+`, un dossier, un `.yml`, `infra/`).
- **À proscrire** : dates, numéros de PR, récits d'incident, volumes ou pourcentages relevés à un
  instant, « en cours », « temporaire », noms de personnes (écrire le rôle), identifiants internes
  (buckets, comptes de service, chemins), « remplace… », « anciennement… ». L'historique va dans
  le message de commit et la description de PR.

Avant d'écrire un fait, se demander : **sera-t-il encore vrai dans six mois sans que personne n'y
touche ?** Sinon, ne pas l'écrire, ou en faire un test dbt.

Les descriptions YAML partent dans BigQuery (`persist_docs`) et la doc dbt est publique ; le data
analyst et l'agent text-to-SQL les lisent : un chiffre périmé y devient une fausse information.

### Descriptions : dans le YAML seulement

La description d'un modèle vit dans son YAML, à toutes les couches, jamais dans le `config()`.
Seule celle du YAML est publiée (doc dbt, BigQuery, agents), et deux copies finissent par
diverger. La règle `dbt check` `description_dans_config` arrête le build sinon.

---

## Power BI

Power BI lit `prod_marts`. Un rapport qui lit un `int_*` ou un seed est une dette, à résorber en
mart. Un mart lu par Power BI est une **table** : une vue exigerait du compte de Power BI des droits
sur toute sa chaîne amont. Les rapports joignent les tables sur `<entite>_id`, et chacun est
déclaré dans `models/exposures/<bu>.yml`.
