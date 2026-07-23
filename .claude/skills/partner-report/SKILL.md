---
name: partner-report
description: Analyse récurrente mensuelle par partenaire (interventions, volume prod, €, ETP équivalent technicien) sur fct_technique__intervention. Lecture seule BigQuery prod_marts. Utile pour suivre un partenaire dans le temps ou chiffrer l'impact d'une perte/gain de volume (ex. BRITA, TWYD, NESPRESSO...).
argument-hint: "[partenaires] [mois_debut] [mois_fin] (ex. 'BRITA,TWYD 2026-01 2026-06' ou vide = tous les partenaires depuis 2025-01)"
user-invocable: true
---

# Partner Report — Analyse mensuelle par partenaire

Reporting récurrent sur `fct_technique__intervention` (mart technique, `prod_marts`) :
interventions, volume prod, montant facturé et équivalent ETP technicien, **par
mois et par partenaire**. Conçu pour être relancé chaque mois avec les mêmes
défauts (tous les partenaires, depuis 2025-01, lecture seule).

## Arguments

Tous optionnels, positionnels :

- `$0` — liste de partenaires séparés par virgule (ex. `BRITA,TWYD`). Omis ou
  `ALL` → tous les partenaires (`NESPRESSO`, `NESHU`, `BRITA`, `AUUM`, `TWYD`,
  `EXPRESSO`, `NU`, `FONTAINCO`, `DAAN`).
- `$1` — mois de début au format `YYYY-MM`. Défaut : `2025-01`.
- `$2` — mois de fin au format `YYYY-MM`. Défaut : **dernier mois calendaire
  complet** par rapport à la date du jour (ex. si on est le 2026-07-23, le
  dernier mois complet est `2026-06`).

### Exemples

```
/partner-report                              → tous les partenaires, 2025-01 → dernier mois complet
/partner-report BRITA,TWYD                   → BRITA + TWYD, 2025-01 → dernier mois complet
/partner-report BRITA,TWYD 2026-01 2026-06   → reproduit un scénario de perte de volume S1 2026
/partner-report NESPRESSO 2025-06 2025-12    → NESPRESSO sur une plage bornée
```

## Steps

### 1. Résoudre les arguments

- Parser `$0` en liste de partenaires (`upper()`, trim). Vide/`ALL` → pas de
  filtre `partenaire`.
- `$1` → `start_date = <YYYY-MM>-01`.
- `$2` → `end_date = dernier jour du mois <YYYY-MM>`. Si absent, calculer le
  dernier mois calendaire complet à partir de la date courante de la session
  (voir `currentDate` dans le contexte).

### 2. Exécuter la requête BigQuery (lecture seule uniquement)

Projet toujours `evs-datastack-prod`, dataset `prod_marts` (+ `prod_reference`
pour les jours fériés). **Aucune écriture, jamais.**

Outil préféré : `mcp__bigquery__execute_sql_readonly` (utilisé par `/profile`,
`/audit-sources`, `/audit-docs`). S'il n'est pas connecté dans la session,
fallback en `bq` CLI :

```bash
gcloud auth activate-service-account --key-file="$DBT_BIGQUERY_KEYFILE"   # SA du .env, lecture seule sur prod_marts
bq query --use_legacy_sql=false --project_id=evs-datastack-prod --max_rows=2000 --format=csv "<requête>"
```

⚠️ **Piège vécu** : `bq query` tronque silencieusement à 100 lignes par défaut (pas
d'erreur, pas de warning). Avec 9 partenaires × 18 mois ça dépasse vite 100
lignes, et le partenaire trié en dernier (alphabétique) disparaît sans le
moindre signal (`TWYD` a ainsi disparu entièrement d'un premier run alors qu'il
avait bien de la donnée). Toujours passer `--max_rows` largement au-dessus du
nombre de lignes attendu (nb partenaires × nb mois), et vérifier après coup
`SELECT COUNT(DISTINCT partenaire)` / nombre de lignes retournées contre ce qui
est attendu avant de présenter le rapport.

Requête (adapter `start_date`/`end_date`/filtre partenaire) :

```sql
with calendrier as (
    select d as jour, date_trunc(d, month) as mois
    from unnest(generate_date_array(date('{{start_date}}'), date('{{end_date}}'))) as d
),
ouvres as (
    select c.jour, c.mois
    from calendrier c
    left join `evs-datastack-prod.prod_reference.ref_general__feries_metropole` f
        on c.jour = f.date_ferie
    where extract(dayofweek from c.jour) not in (1, 7)  -- exclut samedi/dimanche
      and f.date_ferie is null
),
jours_ouvres_mois as (
    select mois, count(*) as nb_jours_ouvres
    from ouvres
    group by mois
),
base as (
    select
        partenaire,
        tech_secteur,
        date_trunc(date(date_debut), month) as mois,
        count(*) as nb_interventions,
        sum(prod) as nb_prod,
        sum(montant_avec_bonus) as montant_eur
    from `evs-datastack-prod.prod_marts.fct_technique__intervention`
    where date(date_debut) between date('{{start_date}}') and date('{{end_date}}')
      -- and partenaire in ('BRITA', 'TWYD')  -- ajouter uniquement si $0 filtre des partenaires
    group by partenaire, tech_secteur, mois
),
avec_etp as (
    select
        b.*,
        j.nb_jours_ouvres,
        case when b.tech_secteur in ('evs paris 1', 'evs paris 2') then 8 else 6 end as capacite_prod_jour,
        safe_divide(
            b.nb_prod,
            j.nb_jours_ouvres * case when b.tech_secteur in ('evs paris 1', 'evs paris 2') then 8 else 6 end
        ) as etp
    from base as b
    left join jours_ouvres_mois as j on b.mois = j.mois
)
select
    partenaire,
    mois,
    sum(nb_interventions) as nb_interventions,
    sum(nb_prod) as nb_prod,
    round(sum(montant_eur), 2) as montant_eur,
    round(sum(case when tech_secteur is not null then etp else 0 end), 3) as etp_equivalent,
    sum(case when tech_secteur is null then nb_interventions else 0 end) as nb_interventions_secteur_inconnu
from avec_etp
group by partenaire, mois
order by partenaire, mois
```

- `tech_secteur is null` → exclu du calcul ETP (pas de règle 8/6 applicable) mais
  reste compté dans `nb_interventions` / `nb_prod` / `montant_eur` — reporter le
  volume exclu (`nb_interventions_secteur_inconnu`) plutôt que de le passer sous
  silence.
- `montant_avec_bonus` peut être `NULL` côté NESPRESSO pour les états non
  facturables (~5% des lignes) — `SUM` les ignore nativement, pas de traitement
  particulier requis, mais le mentionner si le partenaire filtré est NESPRESSO/NESHU.

### 3. Présenter le rapport

Un tableau mensuel par partenaire (trié par mois croissant), puis un total sur
toute la période demandée, trié par volume prod décroissant :

```
# Analyse partenaires — fct_technique__intervention
Période : 2025-01 → 2026-06 (dernier mois complet) · Lecture seule BigQuery prod_marts

## BRITA
| Mois    | Interventions | Prod | €       | ETP éq. |
|---------|---------------|------|---------|---------|
| 2025-01 | ...           | ...  | ...     | ...     |
| ...     |               |      |         |         |

## TWYD
| Mois    | Interventions | Prod | €       | ETP éq. |
...

## Total période (tous partenaires demandés)
| Partenaire | Interventions | Prod  | €        | ETP éq. moyen/mois |
|------------|---------------|-------|----------|--------------------|
| BRITA      | ...           | ...   | ...      | ...                |
| TWYD       | ...           | ...   | ...      | ...                |

## Notes qualité données
- N interventions sans tech_secteur exclues du calcul ETP (comptées dans interventions/prod/€).
- Montant NULL ignoré par SUM (état non facturable) — concerne surtout NESPRESSO.
```

### 4. Version visuelle (optionnel)

Si l'utilisateur veut un rendu graphique (tendance mensuelle, comparaison
partenaires), proposer de le construire avec le skill `dataviz` en artifact HTML
— ne pas le faire par défaut à chaque exécution récurrente, ça alourdit un
reporting mensuel censé rester rapide.

## Important notes

- **Lecture seule, toujours** — uniquement des `SELECT`. Jamais de `dbt build`/`run`
  contre `prod`, jamais d'écriture BigQuery. Le projet interrogé est
  systématiquement `evs-datastack-prod` (`prod_marts` a les données les plus
  fiables et complètes ; `dev_marts` peut être partiel/obsolète selon les
  derniers builds dev).
- **Jours ouvrés** = lundi-vendredi hors jours fériés de
  `ref_general__feries_metropole` (seed dédiée, pas de calcul de Pâques à la main).
- **ETP** = volume prod / (jours ouvrés du mois × capacité prod/jour du secteur
  technicien réalisant l'intervention : 8 pour `evs paris 1` / `evs paris 2`,
  6 pour les autres secteurs). Répartition sur `tech_secteur`, pas sur le site
  client — c'est un équivalent de capacité théorique, pas un engagement RH
  (ne tient pas compte d'une réaffectation possible des techniciens).
- **`montant_avec_bonus`** est la référence € par défaut (montant facturé +
  bonus délai technicien) — cohérent avec l'analyse BRITA/TWYD S1 2026 déjà
  produite. Si l'utilisateur veut le montant hors bonus, utiliser `montant`.
- Liste des `partenaire` valides à ce jour : `NESPRESSO`, `NESHU`, `BRITA`,
  `AUUM`, `TWYD`, `EXPRESSO`, `NU`, `FONTAINCO`, `DAAN` — si un filtre ne
  matche aucune ligne, le signaler plutôt que de rendre un rapport vide en silence.
