# Architecture — Power BI activity (`powerbi_activity`)

> Dernière mise à jour : 2026-09-03
> **État : source en production. Staging construit (4 modèles) le 2026-09-03 ;
> intermediate/marts à faire, et l'étape dbt reste à ajouter au workflow infra.**

---

## Vue d'ensemble

`powerbi_activity` expose les **journaux d'administration du locataire Power BI** :
qui a consulté quel rapport et quand, plus l'inventaire complet des espaces de
travail, rapports et modèles sémantiques.

Objectif métier : piloter l'usage réel du parc de rapports — lesquels servent,
lesquels sont morts, quelle direction les utilise, et quel coût de
rafraîchissement porte un rapport que personne n'ouvre.

Donnée clé exposée :
- **Événements d'activité** (`powerbi_activity_events`) — 1 ligne = 1 événement
  d'audit, identifié par `id` (GUID du journal d'audit unifié)
- **Inventaire** (`powerbi_workspaces`, `powerbi_reports`, `powerbi_datasets`) —
  snapshot quotidien, 1 ligne = 1 objet du locataire

Source alimentée par l'API d'administration Power BI via dlt
(`ingestion/pipelines/powerbi_activity`). Volume faible : ~2 100 événements pour
27 jours, ~80 événements/jour.

> Voir `docs/pipeline-schedule.md` pour le cron et l'orchestration.

---

## Flux de données

```
┌────────────────────────┐                  ┌──────────────────────────────────┐
│ API admin Power BI     │  dlt             │  prod_raw                        │
│ api.powerbi.com        │ ───────────────► │  powerbi_activity_events (merge) │
│ /v1.0/myorg/admin/     │  elt-powerbi-    │  powerbi_workspaces    (replace) │
│                        │  activity        │  powerbi_reports       (replace) │
│ 4 endpoints            │  7 j/7 à 03:30   │  powerbi_datasets      (replace) │
└────────────────────────┘                  └──────────────┬───────────────────┘
                                                           │ dbt staging
                                                           ▼
                                              prod_staging
                                              stg_powerbi_activity__events
                                              stg_powerbi_activity__workspaces
                                              stg_powerbi_activity__reports
                                              stg_powerbi_activity__datasets
```

---

## ⚠️ TROIS FILTRES OBLIGATOIRES au staging

**C'est le point le plus important de ce document.** Le raw est délibérément
fidèle à la source (règle n°3 d'`ingestion/`) : il contient des artefacts
techniques que Power BI génère lui-même. Sans ces filtres, **les chiffres sont
faux, et faux dans le sens alarmiste**.

| Filtre | Table | Pourquoi |
|---|---|---|
| `app_id is null` | `powerbi_reports` | Power BI crée une **copie** de chaque rapport publié dans une App, avec un `id` distinct et un `app_id` renseigné. Les événements `ViewReport` référencent **toujours l'original** du workspace : les copies affichent donc 0 vue et passent pour dormantes. 29 copies sur 75 rapports. |
| `name not ilike '%usage metrics report%'` | `powerbi_reports` | Rapports de métriques d'usage générés automatiquement par Power BI. 10 au 2026-09-03. |
| `type = 'Workspace' and state = 'Active'` | `powerbi_workspaces` | Les espaces personnels portent **DEUX** types distincts (`PersonalGroup` **et** `Personal`) — filtrer sur le seul `PersonalGroup` en laisse passer. Deux espaces sont par ailleurs `Deleted`. |

**L'écart mesuré** : sans les filtres, 55 rapports « jamais consultés » sur 75.
Avec, **17 sur 36**. Les 38 points d'écart sont intégralement des artefacts.

Un premier tableau de bord a été publié avec les mauvais chiffres avant que
l'erreur soit trouvée. Ne pas refaire ce chemin.

**Et pour mesurer l'usage humain : `activity = 'ViewReport'` uniquement.**
`RefreshDataset` domine le volume d'événements (~80 % ) mais c'est du
rafraîchissement automatique, pas de l'usage. Il reste précieux pour autre
chose : c'est un monitoring gratuit de nos propres pipelines, et le croisement
`rapport dormant × modèle rafraîchi` donne le coût de maintien d'un rapport que
personne ne lit.

---

## Grain et clés

| Table | Grain | Clé | Écriture |
|---|---|---|---|
| `powerbi_activity_events` | 1 événement d'audit | `id` (unique, non nul — vérifié : 2 135 lignes / 2 135 ids) | `merge` |
| `powerbi_workspaces` | 1 espace de travail | `id` | `replace` |
| `powerbi_reports` | 1 rapport (copies App incluses) | `id` ; `workspace_id` → workspaces ; `dataset_id` → datasets ; `original_report_object_id` → le rapport source quand `app_id` est renseigné | `replace` |
| `powerbi_datasets` | 1 modèle sémantique | `id` | `replace` |

Jointure des événements vers l'inventaire : `report_id`, `workspace_id`,
`dataset_id`. **Attention**, ces colonnes ne sont renseignées que sur les
événements qui les concernent (12,6 % pour `report_id`) — un `inner join`
écraserait les autres types d'activité.

---

## Points d'attention

**L'API est incohérente sur la casse.** Elle rend `WorkspaceId` mais
`WorkSpaceName`, ce qui donne `workspace_id` face à **`work_space_name`** après
normalisation dlt. Ce n'est pas une faute de frappe du pipeline.

**Trois tables enfants** dérivées de tableaux imbriqués :
`powerbi_activity_events__schedules__days`, `__schedules__time`,
`__models_snapshots`. Faible volume, liées au parent par `_dlt_parent_id` →
`_dlt_id`. Elles n'existent que si un événement de rafraîchissement planifié
apparaît dans la fenêtre — **ne pas construire de modèle qui en dépende sans
gérer leur absence**.

**La forme d'un événement dépend de son type d'activité.** Les colonnes
`report_*`, `app_*`, `distribution_method`, `consumption_method` n'existent que
sur les consultations. Elles sont **épinglées côté extraction** précisément pour
que leur présence dans le schéma soit garantie même sur une journée sans
consultation — le contrat est donc stable, mais les valeurs sont massivement
nulles.

**Rétention de 27 jours à la source, et c'est irréversible.** L'API ne conserve
rien au-delà. `prod_raw` est donc la **seule** archive : un `--full-refresh` sur
cette source détruirait un historique non reconstituable. Interdit ici comme
côté ingestion.

**Freshness dbt applicable**, contrairement à `zoho_desk` : `_extracted_at` est
un vrai `TIMESTAMP` (et non `_dlt_load_id`, string). C'est le `loaded_at_field`
à déclarer.

**Partition et conservation** : `powerbi_activity_events` est partitionnée par
jour sur `creation_time`, avec expiration à **365 jours**. Les modèles aval
gagnent à filtrer sur `creation_time` pour bénéficier de l'élagage de partition.

---

## Données personnelles — contrainte de modélisation

Les événements identifient nominativement l'utilisateur (`user_id`, `user_key`).
Le dispositif relève du **suivi d'activité des salariés**, avec une finalité
déclarée : rationaliser le parc de rapports, **pas** évaluer les personnes.

> ### ⚠️ Arbitrage du 2026-09-03 — le nominatif EST exposé
>
> La règle initiale de cette section était « exposer des agrégats, pas des
> identités ». **Le propriétaire du dépôt a arbitré autrement le 2026-09-03** :
> savoir qui lit quoi est jugé utile au pilotage du parc. Cette section est
> conservée pour mémoire du raisonnement, mais la règle appliquée est
> désormais celle-ci :
>
> | Mart | Nominatif ? |
> |---|---|
> | `fct_bi__consultation` | **Oui** — `user_id` en clair, 1 ligne = telle personne a ouvert tel rapport tel jour |
> | `fct_bi__activite_rapport_jour` | Non — `count(distinct user_id)` |
> | `fct_bi__usage_rapport` | Non — `count(distinct user_id)` |
> | `dim_bi__rapport` | `created_by` / `modified_by` = UPN d'**auteur** (métadonnée d'objet, pas de la consommation) |
>
> Deux limites tenues volontairement : `client_ip` et `user_agent` ne sont
> **pas** remontés en marts (la demande porte sur qui consulte quoi, pas sur
> d'où ni avec quel appareil), et ils restent disponibles dans
> `stg_powerbi_activity__events` si le besoin apparaît.
>
> **Ce que l'arbitrage change** : le dispositif devient un traitement de
> données personnelles à part entière, et non plus une mesure d'audience
> anonymisée. Registre des traitements, information des salariés et arbitrage
> CSE ne sont plus des précautions théoriques — voir
> `ingestion/pipelines/powerbi_activity/HABILITATION.md`. La finalité déclarée
> (rationaliser le parc, pas évaluer les personnes) reste l'invariant : c'est
> elle qui délimite les usages légitimes de `fct_bi__consultation`.

Repères mesurés au 2026-09-03 : 32 lecteurs distincts pour 246 consultations,
soit 7,7 consultations par personne en moyenne sur 27 jours ; 4 personnes n'ont
ouvert qu'un seul rapport une seule fois. Deux domaines coexistent
(`evs-pro.com`, `neshu.com`), ce qui donne un axe de segmentation par entité
sans désigner personne (`user_domain`). Les 246 consultations sont toutes en
`user_type = 0` : aucun compte de service n'ouvre de rapport.

---

## Chiffres de référence — pour valider un modèle

Relevé du 2026-09-03, fenêtre 2026-08-07 → 2026-09-02 (27 jours) :

| Mesure | Valeur |
|---|---|
| Événements | 2 135 (2 135 ids distincts) |
| dont `ViewReport` | 246 |
| Espaces de travail (tous) | 149 — dont **13** partagés actifs |
| Rapports (inventaire brut) | 139 — dont **36** métier après filtres |
| Rapports métier consultés | 19 |
| Rapports métier dormants | **17** |
| Utilisateurs distincts | 32 |
| Rafraîchissements de modèles sans lecteur | 312 |

Un staging correct doit retrouver 36 rapports métier et 13 espaces partagés.
S'il en trouve 75 et 149, les filtres ne sont pas appliqués.

---

## À faire

1. ~~**`_powerbi_activity__sources.yml`**~~ — **FAIT 2026-09-03.** 4 tables,
   `loaded_at_field: _extracted_at`, freshness native 26h/48h (tier Standard),
   pattern `staging/zoho_desk/`. Les 3 tables enfants
   (`__schedules__days`, `__schedules__time`, `__models_snapshots`) sont
   **volontairement non déclarées** : un `source()` sur une table absente casse
   le build, et elles n'existent que si un rafraîchissement planifié tombe dans
   la fenêtre.
2. ~~**Staging**~~ — **FAIT 2026-09-03.** 4 modèles `table`, 52 tests (0 erreur,
   4 warnings attendus et documentés). Les filtres **intra-table** y sont :
   `app_id is null` + exclusion « usage metrics report » sur `__reports`,
   `type = 'Workspace' and state = 'Active'` sur `__workspaces`.

   ⚠️ **Le périmètre « rapports métier » n'est PAS atteint par le staging seul.**
   Les 36/37 rapports métier exigent en plus la restriction aux espaces partagés
   actifs, soit une **jointure reports × workspaces** — interdite en staging
   (`docs/conventions/staging.md` § 1 et § 9). `stg_powerbi_activity__reports`
   sort donc **100** lignes, pas 37. Décomposition mesurée au 2026-09-03 :

   | Étape | Lignes |
   |---|---|
   | rapports bruts | 139 |
   | dans un espace partagé actif | 76 |
   | hors copies d'App (`app_id is null`) | 47 |
   | hors métriques d'usage | **37** |
   | dont consultés / dormants | 19 / 18 |

   Le staging garantit les deux filtres intra-table (tests
   `expression_is_true`) ; **la jointure de périmètre appartient à
   l'intermediate**, et c'est là que le chiffre de 37 doit être vérifié.
3. ~~**Marts**~~ — **FAIT 2026-09-03**, dans une **nouvelle BU `bi`**
   (`models/marts/bi/`) : gouvernance du parc Power BI, télémétrie de la
   plateforme BI elle-même et non un domaine métier. **Pas de couche
   intermediate** : la logique tient en une jointure de périmètre et deux
   agrégats.

   | Mart | Grain | Rôle |
   |---|---|---|
   | `dim_bi__rapport` | 1 rapport (37) | le parc — c'est ici que la jointure interne sur les espaces partagés actifs matérialise le 3ᵉ filtre obligatoire, et fait passer de 100 à 37 |
   | `fct_bi__activite_rapport_jour` | (jour × rapport) (684) | série temporelle : consultations, utilisateurs distincts, rafraîchissements |
   | `fct_bi__usage_rapport` | 1 rapport (37) | **le livrable** : dormance, dernière consultation, coût de rafraîchissement |
   | `fct_bi__consultation` | 1 consultation (246) | **nominatif** : qui a ouvert quoi, quand, depuis quel client (arbitrage du 2026-09-03) |

   Propriété structurante mesurée : **le parc est en 1:1 rapport / modèle
   sémantique** (37 ↔ 37). C'est ce qui autorise à rattacher au rapport des
   rafraîchissements portés par le modèle, sans double comptage. L'hypothèse
   est verrouillée par un test `unique` sur `dim_bi__rapport.dataset_id` —
   si un modèle se met à servir deux rapports, le test casse avant que les
   mesures soient faussées.

   `activity = 'ViewReport'` est appliqué **ici**, pas au staging, qui conserve
   toutes les activités à dessein.

   Aucune `exposure` créée : aucun rapport Power BI ne consomme encore ces
   marts. À créer (`models/exposures/bi.yml`) le jour où le tableau de bord de
   gouvernance existe.
4. **Ajouter l'étape dbt au workflow.** `infra/workflows/pipeline-powerbi-activity.yaml`
   n'a **qu'une étape EL** aujourd'hui : le staging existe mais **rien ne le
   construit en prod**. Conformément à l'architecture Option C, y ajouter
   `dbt build source:powerbi_activity+` sur le modèle de
   `pipeline-yuman-evs.yaml`.

---

## Dérive des chiffres de référence — relevé du 2026-09-03 (build)

La fenêtre de 27 jours glisse : les chiffres du relevé initial
(2026-08-07 → 2026-09-02) ont bougé d'un jour au moment du build. Les écarts
ci-dessous sont **normaux**, pas des filtres cassés — les deux ancres qui
prouvent les filtres (13 espaces partagés, 19 rapports consultés) sont exactes.

| Mesure | Relevé initial | Build 2026-09-03 |
|---|---|---|
| Événements | 2 135 | 2 135 |
| dont `ViewReport` | 246 | 248 |
| Espaces partagés actifs | **13** | **13** |
| Rapports métier | 36 | 37 |
| Rapports métier consultés | **19** | **19** |
| Rapports métier dormants | 17 | 18 |
| Utilisateurs distincts | 32 | 34 |
