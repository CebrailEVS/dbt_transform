# Architecture — GAC (`gac`)

> Dernière mise à jour : 2026-05-24

---

## Vue d'ensemble

`gac` expose le **suivi des sinistres véhicules** de la flotte EVS, géré par
le prestataire d'assurance **GAC**. Le périmètre couvre l'ensemble des
sinistres déclarés sur les véhicules d'entreprise : circonstances, expertise,
réparation, coûts (assureur / franchise / global / facturé client),
collaborateur impliqué.

Donnée clé exposée :
- **Sinistres** (`gac_suivi_sinistres_sg`) — 1 ligne = 1 sinistre, identifié
  soit par `n_de_sinistre` (interne GAC) soit par `reference_gac`

Source mono-table, alimentée par un **fichier déposé sur le SFTP EVS** par
GAC, ingéré via Singer (`tap-sftp-gac-sinistre`). Volume faible (~240 lignes
courant 2026).

> Voir `docs/pipeline-schedule.md` pour le cron et l'orchestration.

---

## Flux de données

```
┌──────────────────────┐  SFTP EVS         ┌─────────────────────────────┐
│  GAC                 │ ────────────────► │  prod_raw                   │
│  (assureur flotte)   │  tap-sftp-gac-    │  gac_suivi_sinistres_sg     │
│                      │  sinistre         │                             │
└──────────────────────┘                   └──────────┬──────────────────┘
                                                      │ dbt staging
                                                      ▼
                                       ┌──────────────────────────────────┐
                                       │  prod_staging                    │
                                       │  stg_gac__sinistres_sg           │
                                       └──────────┬───────────────────────┘
                                                  │ dbt marts (direct)
                                                  ▼
                                       ┌──────────────────────────────────┐
                                       │  marts/services_generaux/        │
                                       │  fct_services_generaux__sinistre │
                                       └──────────────────────────────────┘
```

**Pas de couche intermediate** — le staging est consommé directement par
l'unique mart `fct_services_generaux__sinistre`.

---

## Le modèle staging — `stg_gac__sinistres_sg`

Grain : **1 ligne par sinistre**.

**Volumétrie (mai 2026)** :
- 238 lignes, 191 `n_de_sinistre` distincts
- 47 lignes sans `n_de_sinistre` (rattrapées par `reference_gac` — 0 ligne
  sans aucun des deux)
- Plage `date_sinistre` : 2021-01-08 → 2026-05-22
- Statuts : `Clos` (159), `A la route` (79)

### Structure

| Catégorie | Colonnes (extrait) |
|---|---|
| **Identifiants** | `n_de_sinistre`, `reference_gac`, `immat` |
| **Statut / vie du dossier** | `statut_actuel` (Clos / À la route), `cloture`, `date_cloture_sinistre` |
| **Circonstances** | `type_sinistre`, `circonstance`, `description`, `tiers`, `resp` (0-100), `lieu_du_sinistre`, `trajet_domicile_travail`, `accident_de_week_end` |
| **Collaborateur** | `matricule`, `nom`, `prenom`, `centre_de_couts`, `entite_entite_1..4` |
| **Véhicule** | `immat`, `genre_fiscal` |
| **Coûts** (float64) | `cout_assureur`, `auto_assurance`, `franchise`, `cout_global`, `cout_client` |
| **Dates expertise / réparation** | `date_passage_expert`, `date_reception_constat`, `date_envoie_constat_assureur`, `date_debut_reparation`, `date_fin_reparation`, `date_remise_vehicule_collaborateur` |
| **Méta Meltano** | `_sdc_source_file`, `_sdc_source_lineno`, `_sdc_received_at`, `_sdc_batched_at`, `_sdc_sequence` |

Configuration :
```sql
{{ config(materialized='table', description='...') }}
```

---

## Mart consommateur

| Modèle | Rôle |
|---|---|
| `fct_services_generaux__sinistre` | Fait pivot du domaine services généraux — sinistres flotte avec contexte véhicule / collaborateur / coûts. Wrapper léger sur le staging (sélection / renommage `date(date_sinistre)`), pas d'enrichissement. |

Pas de dimension dédiée — les libellés véhicule, collaborateur et centre de
coûts restent dénormalisés dans le fait (cohérent avec la simplicité de la
source).

---

## Points d'attention

### Source CSV avec noms de colonnes corrompus
Le fichier GAC arrive avec des accents et caractères spéciaux dans les
en-têtes (ex. `coût global`, `clôturé`, `référence GAC`, `Immat.`,
`expert téléphone`). Le normalisateur Meltano les remplace par des
underscores, d'où des colonnes brutes type `co_t_global`, `cl_tur_`,
`r_f_rence_gac`, `immat_`, `expert_t_l_phone`, `co_t_assureur`,
`co_t_client`, `pr_nom`, `centre_de_co_ts`, `date_de_cr_ation`,
`date_de_cl_ture_du_sinistre`, etc. Le staging les **renomme proprement**
(`cout_global`, `cloture`, `reference_gac`, `immat`, `prenom`,
`centre_de_couts`, `date_de_creation`, `date_cloture_sinistre`…).
**Toujours partir du staging**, jamais de la source brute.

### Déduplication par PK alternative (`n_de_sinistre` ou `reference_gac`)
20 % des lignes (47 / 238) n'ont pas de `n_de_sinistre` mais portent un
`reference_gac`. Le staging utilise une **clé de dédoublonnage conditionnelle**
via `coalesce`-style en `case when` :
```sql
partition by case
    when n_de_sinistre is not null then n_de_sinistre
    when reference_gac is not null then reference_gac
end
order by _sdc_received_at desc
```
**Implication** : les marts ne doivent jamais utiliser `n_de_sinistre` seul
comme PK. Pour relier deux sources sur le sinistre, préférer
`coalesce(n_de_sinistre, reference_gac)`.

### Dates au format français `dd/mm/yyyy [hh:mm:ss]`
Le CSV expose toutes les dates en format français. Le staging utilise
`safe.parse_timestamp('%d/%m/%Y %H:%M:%S', ...)` pour les timestamps
(`date_sinistre`, `date_de_creation`) et `safe.parse_date('%d/%m/%Y', ...)`
pour les dates annexes (expertise, réparation, clôture). Les valeurs
malformées passent en NULL plutôt qu'en erreur. Surveiller via
`countif(date_sinistre is null)` si la qualité du CSV se dégrade.

### Coûts forcés en `float64`
Les colonnes coûts (`cout_assureur`, `auto_assurance`, `franchise`,
`cout_global`, `cout_client`) sont **directement** castées en `float64`
**sans `safe_cast`** dans le staging. Si une valeur n'est pas castable
(caractère parasite, virgule décimale FR non gérée), BigQuery lèvera une
erreur dure. À durcir si un incident d'extraction survient (passer en
`safe_cast` + log des valeurs rejetées).

### Volumétrie très faible — pas de partition / cluster
~240 lignes au total, croissance lente (quelques dizaines par an).
Pas de partition ni cluster configurés sur le staging — inutile à cette
échelle. Si la source grossit fortement (intégration d'historique étendu,
ajout d'autres prestataires assurance), envisager une partition sur
`date_sinistre`.

### Pas de freshness configurée (`freshness: null`)
`_gac__sources.yml` désactive explicitement la freshness (`freshness: null`).
Cohérent avec le tier *Relaxe* (7j / 14j) de cette source — `dbt source
freshness` n'alerte pas dessus.
