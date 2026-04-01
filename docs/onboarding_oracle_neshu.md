# Guide d'onboarding — Données Oracle Neshu & Architecture dbt

**Destinataire :** Stagiaire Data Analyst  
**Auteur :** Équipe Data Engineering  
**Dernière mise à jour :** 2026-04-01

---

## À quoi sert ce document ?

Ce guide t'explique la source de données Oracle Neshu : ce qu'elle représente métier,
comment sa base de données brute est structurée, et comment on l'a transformée en un
entrepôt de données propre et analysable avec dbt. Lis-le du début à la fin — chaque
section s'appuie sur la précédente.

---

## 1. Contexte métier — c'est quoi Oracle Neshu ?

Oracle Neshu est le principal ERP opérationnel utilisé par EVS Professionnelle France.

EVS déploie et entretient des **machines à café et boissons** (appelées *devices*) chez ses
clients (bureaux, hôtels, restaurants, etc.). Les opérations du quotidien sont :

- **Livrer** des produits consommables (capsules café, thés, chocolats, gobelets…) chez les clients
- **Approvisionner les machines** sur site via des passages roadman (chargement de produits directement dans la machine)
- **Réceptionner du stock** de fournisseurs dans les entrepôts
- **Déplacer du stock** en interne entre entrepôts, véhicules et zones de préparation
- **Suivre la consommation** de produits via la télémétrie ou les lectures roadman
- **Gérer des interventions techniques** (maintenance, réparation, détartrage…)
- **Suivre l'activité des roadmen** (pointage, durée de passage, planifié vs. réalisé)

Tout cela est enregistré dans Oracle Neshu sous forme de **tâches** réalisées par des
**ressources** (roadmen & véhicules) sur des **machines** appartenant à des **sociétés**
(clients), consommant des **produits**.

---

## 2. La base de données Oracle Neshu brute — les tables clés

Avant toute transformation, les données brutes vivent dans BigQuery dans le dataset
`prod_raw`, sous la source `oracle_neshu`. Voici les tables les plus importantes.

### 2.1 Les entités principales (les « noms »)

| Table brute | Ce qu'elle représente |
|---|---|
| `evs_company` | Un client, fournisseur ou entité interne (un dépôt) |
| `evs_device` | Une machine déployée chez un client |
| `evs_product` | Un article consommable (capsule café, thé, gobelet, sirop…) |
| `evs_resources` | Une personne (roadman) ou un véhicule — les deux sont des « ressources » dans Neshu |
| `evs_location` | Une adresse physique/site lié à une société ou une machine |
| `evs_contract` | Un engagement commercial entre EVS et une société cliente |
| `evs_task` | **La table de faits centrale** — chaque opération enregistrée dans l'ERP |
| `evs_task_has_product` | Quels produits ont été déplacés/consommés dans une tâche, avec les quantités et les prix |
| `evs_task_has_resources` | Quels roadmen/véhicules ont été affectés à une tâche |

> **Point clé :** presque toute l'analyse métier part de `evs_task`. Les tâches sont les
> événements — tout le reste (société, machine, produit, ressources) apporte du contexte
> à ces événements.

### 2.2 Les tables de référence

| Table brute | Ce qu'elle représente |
|---|---|
| `evs_task_type` | Le type de tâche (livraison, chargement, télémétrie, inventaire, etc.) |
| `evs_task_status` | Le statut d'une tâche (FAIT, VALIDE, ANNULE…) |
| `evs_resources_type` | Si une ressource est une personne (type 2) ou un véhicule (type 3) |
| `evs_company_type` | La catégorie d'une société |
| `evs_product_type` | La catégorie d'un produit |

Ces tables changent rarement et servent à décoder les IDs en libellés lisibles.

### 2.3 Le système de Labels (EAV) — la partie complexe

Oracle Neshu utilise un système de **labels** flexible pour attacher des attributs aux
entités sans modifier le schéma de la base de données. C'est un pattern courant appelé
**EAV (Entity-Attribute-Value)**.

Au lieu d'avoir une colonne `region` directement sur `evs_company`, Neshu stocke
l'information comme ceci :

```
evs_label_family  →  code = 'REGION'
evs_label         →  code = 'NORD', idlabel_family = (id de REGION)
evs_label_has_company  →  idcompany = 123, idlabel = (id de NORD)
```

Pour connaître la région d'une société, il faut donc joindre **3 tables**. Cela donne à
Neshu la flexibilité d'ajouter de nouveaux attributs sans migration de schéma, mais
cela rend les requêtes brutes complexes.

**Les principales familles de labels que tu rencontreras :**

| Code famille de label | Ce qu'elle tague | Valeurs possibles (exemples) |
|---|---|---|
| `REGION` | Société | NORD, SUD, EST, OUEST, IDF… |
| `SECTEUR_DACTIVITE` | Société | HORECA, OFFICE, TRAVEL, INDUSTRY… |
| `STATUT_CLIENT` | Société | OR, ARGENT, BRONZE |
| `ISACTIVE` | Société / Machine / Produit / Ressource | yes, no |
| `KA` | Société | yes (flag Compte Clé) |
| `MARQUE` | Machine | Nom de marque de la machine |
| `GAMME` | Machine | Gamme/niveau de la machine |
| `CATEGORIE` | Machine | Catégorie de la machine |
| `MODECOMA` | Machine | Modèle économique (GRATUIT, PAYANT…) |
| `MARQUEP` | Produit | Marque du produit |
| `FAMILLE` | Produit | Famille produit (CAFE CAPSULES, THE, CHOCOLATS…) |
| `GROUPE` | Produit | Groupe produit (BOISSONS FRAICHES, SNACKING, ACCESSOIRES…) |

> **Ne requête jamais les labels directement depuis les tables brutes.** Dans notre projet
> dbt, tout le pivotage des labels est fait pour toi dans les modèles de dimension.
> Voir section 6.

---

## 3. Le projet dbt — architecture en 3 couches

On n'expose jamais les données brutes directement à Power BI. On les transforme à travers
**3 couches**, chacune avec une responsabilité claire.

```
prod_raw (BigQuery)
    └── tables sources oracle_neshu
            │
            ▼
    [Couche STAGING]  prod_staging
        Nettoyer, renommer, caster les colonnes. 1 modèle = 1 table source.
            │
            ▼
    [Couche INTERMEDIATE]  prod_intermediate
        Logique métier. Séparer les tâches par type, enrichir avec ressources & produits.
            │
            ▼
    [Couche MARTS]  prod_marts
        Dimensions et faits finaux, prêts pour Power BI.
```

La règle est simple : **on ne saute jamais une couche**. Les marts ne référencent que des
intermédiaires ou d'autres modèles de staging — jamais des sources brutes directement.

---

## 4. Couche 1 — Staging (`models/staging/oracle_neshu/`)

**Objectif :** rendre les données brutes sûres à utiliser.  
Un modèle staging = une table brute. Rien de plus, rien de moins.

Ce que le staging fait :
- Caste les IDs en `INT64` (ils arrivent en tant que chaînes de caractères dans la couche brute)
- Convertit les chaînes de dates en types `TIMESTAMP` propres
- Renomme les colonnes pour suivre nos conventions (`created_at`, `updated_at`, `extracted_at`, `deleted_at`)
- Filtre pour ne garder que les enregistrements actifs (`code_status_record = '1'`)

Ce que le staging ne fait PAS : pas de logique métier, pas de jointures entre entités.

### Les modèles staging clés à connaître

| Modèle | Ce qu'il fournit |
|---|---|
| `stg_oracle_neshu__company` | Table société propre — mais sans labels pour l'instant |
| `stg_oracle_neshu__device` | Table machine propre — sans labels pour l'instant |
| `stg_oracle_neshu__product` | Table produit propre — sans labels pour l'instant |
| `stg_oracle_neshu__resources` | Table roadmen & véhicules propre |
| `stg_oracle_neshu__task` | **Le modèle staging central** — toutes les tâches, propres et typées. Incrémental (merge). |
| `stg_oracle_neshu__task_has_product` | Produits par tâche avec quantités et prix unitaires. Incrémental (merge). |
| `stg_oracle_neshu__task_has_resources` | Roadmen/véhicules par tâche |
| `stg_oracle_neshu__label` | Table label propre |
| `stg_oracle_neshu__label_family` | Table famille de labels propre |
| `stg_oracle_neshu__label_has_company` | Table de liaison : label → société |
| `stg_oracle_neshu__label_has_device` | Table de liaison : label → machine |
| `stg_oracle_neshu__label_has_product` | Table de liaison : label → produit |
| `stg_oracle_neshu__label_has_task` | Table de liaison : label → tâche |
| `stg_oracle_neshu__label_has_resources` | Table de liaison : label → ressource |
| `stg_oracle_neshu__task_type` | Référentiel : codes de type de tâche (3=Télémétrie, 101=Livraison, 13=Chargement…) |
| `stg_oracle_neshu__task_status` | Référentiel : codes statut (FAIT, VALIDE, ANNULE…) |

> **Important — modèles incrémentaux :** `stg_oracle_neshu__task` et
> `stg_oracle_neshu__task_has_product` sont incrémentaux. Cela signifie qu'à chaque
> exécution dbt, seuls les enregistrements nouveaux ou récemment modifiés sont traités
> (avec une fenêtre de 7 jours). Cela maintient le pipeline rapide malgré un historique
> de tâches volumineux.

---

## 5. Couche 2 — Intermediate (`models/intermediate/oracle_neshu/`)

**Objectif :** appliquer la logique métier et décomposer la table de tâches monolithique
en datasets ciblés.

La table brute `evs_task` contient **tous les types de tâches mélangés**. Une livraison,
un passage d'approvisionnement et une intervention technique coexistent dans la même table.
La couche intermediate les sépare.

Chaque modèle intermediate :
1. Filtre `stg_oracle_neshu__task` sur un `idtask_type` spécifique
2. Joint `stg_oracle_neshu__task_has_product` pour obtenir les produits déplacés
3. Joint optionnellement `stg_oracle_neshu__task_has_resources` pour obtenir le roadman/véhicule
4. Applique les conversions de quantité (coefficients d'unité)
5. Calcule la valorisation (`quantité × prix_unitaire`)

### Types de tâches et leurs modèles intermediates

| ID type de tâche | Signification métier | Modèle intermediate |
|---|---|---|
| `3` | **Télémétrie** — lecture du compteur de la machine | `int_oracle_neshu__telemetry_tasks` |
| `13` | **Chargement** — le roadman charge des produits dans une machine sur site | `int_oracle_neshu__chargement_tasks` |
| `101` | **Livraison** — produits livrés chez un client | `int_oracle_neshu__livraison_tasks` |
| `121` | **Réception** — stock reçu d'un fournisseur | `int_oracle_neshu__reception_tasks` |
| `131` | **Intervention technique** — maintenance, réparation, détartrage | `int_oracle_neshu__inter_techinique_tasks` |
| `132` | **Commande interne** — mouvement entre zones internes | `int_oracle_neshu__commande_interne` |
| `161` | **Livraison interne** — mouvement entre entités internes | `int_oracle_neshu__livraison_interne_tasks` |
| `162` | **Inventaire** — comptage de stock | `int_oracle_neshu__inventaire_tasks` |
| `163` | **Écart inventaire** — enregistrement d'un écart de stock | `int_oracle_neshu__ecart_inventaire_tasks` |
| `194` | **Pointage** — début/fin de journée du roadman | `int_oracle_neshu__pointage` |
| `32` | **Passage appro** — visite du roadman sur une machine | `int_oracle_neshu__appro_tasks` |
| `11` | **Invendus** — redistribution de produits invendus | `int_oracle_neshu__invendus_tasks` |

Il existe aussi deux intermédiaires non liés aux tâches :

| Modèle intermediate | Objectif |
|---|---|
| `int_oracle_neshu__valorisation_parc_machines` | Agrège le nombre de machines et la valeur totale par modèle/groupe |
| `int_oracle_neshu__pointage` | Enregistrements de pointage roadman, utilisés pour les calculs de durée de travail |

### Les coefficients d'unité

Les produits dans Neshu peuvent avoir des coefficients de conversion d'unité. Par exemple,
un produit appelé « GOBELET RAME 50 » représente une rame de 50 gobelets — donc 1 unité
dans Neshu équivaut à 50 gobelets réels.

Les champs concernés sont `unit_coeff_multi` et `unit_coeff_div` sur
`stg_oracle_neshu__task_has_product`.

Les modèles intermédiaires appliquent cette conversion :

```
quantite_ajustee = real_quantity × unit_coeff_multi / unit_coeff_div
```

Dans la couche marts, tu travailleras toujours avec la **quantité ajustée**.

---

## 6. Couche 3 — Marts (`models/marts/oracle_neshu/`)

**Objectif :** tables propres, dénormalisées, prêtes pour Power BI.

La couche marts combine tout : les dimensions portent tous les attributs métier
(y compris les labels pivotés), les faits portent les mesures.

### Les modèles de dimension

Les dimensions décrivent le « qui/quoi » — les entités impliquées dans les opérations.
Les labels sont **pivotés** ici en vraies colonnes, tu n'as donc jamais besoin de
joindre les tables de labels toi-même.

| Modèle | Ce qu'il fournit |
|---|---|
| `dim_oracle_neshu__company` | Sociétés avec région, secteur, statut client (OR/ARGENT/BRONZE), flag KA, tranche de collaborateurs, adresse — tous les attributs labels déjà en colonnes |
| `dim_oracle_neshu__device` | Machines avec marque, gamme, catégorie, modèle économique, flag actif — tous pivotés depuis les labels |
| `dim_oracle_neshu__product` | Produits avec marque, famille, groupe, type produit standardisé (CAFE CAPS, THE, CHOCOLATS…), flag actif |
| `dim_oracle_neshu__contract` | Contrats actifs par société avec engagement nettoyé et nombre de collaborateurs |
| `dim_oracle_neshu__resources` | Roadmen et véhicules avec code GEA, société, coût, dates d'arrivée/départ, flag actif |
| `dim_oracle_neshu__vehicule_roadman` | Véhicules avec leur roadman associé et code GEA — une dimension d'aide pour l'analyse par véhicule |

### Les modèles de fait

Les faits décrivent « ce qui s'est passé » — les mesures, quantités et événements dans le temps.

| Modèle | Ce qu'il mesure | Partitionné par |
|---|---|---|
| `fct_oracle_neshu__conso_business_review` | **Consommation consolidée** sur 3 sources (télémétrie, chargement, livraison) avec contexte société/machine/produit. Le fait de consommation principal. | `consumption_date` |
| `fct_oracle_neshu__pa_business_review` | Résumé des passages appro — planifié vs. réalisé par roadman/société/machine | — |
| `fct_oracle_neshu__appro` | Passages appro détaillés avec durée, classement journalier, heures de travail (nettoyées) | `task_start_date` |
| `fct_oracle_neshu__chargement_vs_conso` | Comparaison chargement vs. consommation télémétrie par machine/produit/fenêtre de date | — |
| `fct_oracle_neshu__chargement_par_quinzaine` | Totaux de chargement bi-hebdomadaires par type de produit et société | — |
| `fct_oracle_neshu__supply_flux` | Flux supply chain mensuel — stocks + réceptions + tous les types de mouvement consolidés | `mois_date` |

---

## 7. Les snapshots — garder l'historique dans le temps

Certaines tables évoluent dans le temps et on veut **conserver l'historique**. Pour cela
on utilise les snapshots dbt (SCD Type 2). Les snapshots sont gérés exclusivement par
l'équipe Data Engineering — tu ne modifieras pas ces fichiers, mais c'est utile de
savoir qu'ils existent.

| Snapshot | Quel historique il trace |
|---|---|
| `snap_oracle_neshu__company` | Changements de nom de société et passage actif/inactif |
| `snap_oracle_neshu__device` | Changements de modèle économique et transferts de machine entre sociétés |
| `snap_oracle_neshu__valo_parc_machines` | Snapshot mensuel de la valorisation du parc machines par modèle/groupe |

Chaque ligne de snapshot possède des colonnes `dbt_valid_from` et `dbt_valid_to`.
Une ligne avec `dbt_valid_to IS NULL` est **l'enregistrement courant**. Les lignes avec
un `dbt_valid_to` non nul sont les versions historiques.

---

## 8. Relations entre entités — la vue d'ensemble

```
SOCIETY ──────────────── CONTRAT
   │                        (un contrat actif par société)
   │  (un-à-plusieurs)
   ├────────── MACHINE
   │               │
   │               │ (un-à-plusieurs)
   │               └────── TÂCHE ──────── TASK_HAS_PRODUCT ─── PRODUIT
   │                          │               (quantités,
   │ (un-à-plusieurs)          │                prix)
   └────────── RESSOURCES      └──── TASK_HAS_RESOURCES
                (roadmen &               (qui a réalisé
                 véhicules)               la tâche)
                   │
                   └──── Le système LABEL ajoute des attributs à toutes les entités ci-dessus
```

**Comment lire ce schéma :**
- Une société peut avoir plusieurs machines, plusieurs contrats, plusieurs roadmen/véhicules
- Une machine est impliquée dans plusieurs tâches dans le temps
- Une tâche peut impliquer plusieurs produits (via task_has_product) et plusieurs ressources (via task_has_resources)
- Les labels ajoutent des attributs flexibles (région, marque, statut…) à presque toutes les entités

---

## 9. Les colonnes que tu verras partout

Chaque modèle staging expose ces 4 colonnes système — tu les trouveras sur tous les modèles :

| Colonne | Signification |
|---|---|
| `created_at` | Quand l'enregistrement a été créé dans Oracle Neshu |
| `updated_at` | Quand l'enregistrement a été modifié pour la dernière fois dans Oracle Neshu |
| `extracted_at` | Quand Meltano a extrait cet enregistrement depuis Oracle (timestamp d'ingestion) |
| `deleted_at` | Quand l'enregistrement a été soft-supprimé dans Oracle (NULL s'il est encore actif) |

Dans les modèles de dimension et de fait, les IDs suivent la convention `<entité>_id`
(ex. `company_id`, `device_id`, `product_id`). Dans les modèles staging, les IDs
conservent la convention de nommage Neshu (`idcompany`, `iddevice`, `idproduct`).

---

## 10. Comment explorer les modèles toi-même

```bash
# Construire un modèle et toutes ses dépendances en amont
dbt build -s +dim_oracle_neshu__company

# Construire tout ce qui est taggé oracle_neshu
dbt build --select tag:oracle_neshu

# Construire uniquement la couche staging pour oracle_neshu
dbt build --select tag:staging,tag:oracle_neshu

# Exécuter un modèle sans les tests (plus rapide pour l'exploration)
dbt run -s dim_oracle_neshu__company

# Voir le SQL compilé d'un modèle (utile pour comprendre ce qu'il fait)
# Après dbt compile, regarder dans target/compiled/
dbt compile -s dim_oracle_neshu__company
```

> **Rappel :** exécute toujours contre la cible `dev` (c'est la valeur par défaut). Cela
> écrit dans `dev_staging`, `dev_intermediate`, `dev_marts` — jamais en production.

---

## 11. Par où commencer en tant que Data Analyst

Voici un ordre de lecture suggéré pour bien démarrer :

1. **Lis les modèles staging** des 3 entités principales : `stg_oracle_neshu__company`,
   `stg_oracle_neshu__device`, `stg_oracle_neshu__product` — ils sont simples et montrent
   la forme brute des colonnes.

2. **Lis `stg_oracle_neshu__task`** — comprends les champs clés : `idtask_type`,
   `idtask_status`, `real_start_date`, `real_end_date`, `iddevice`, `idcompany_peer`.

3. **Lis un modèle intermediate**, par exemple `int_oracle_neshu__chargement_tasks` —
   vois comment il filtre sur `idtask_type = 13` et s'enrichit avec les produits et ressources.

4. **Lis `dim_oracle_neshu__company`** — vois comment le pivot de labels fonctionne en
   pratique. C'est le pattern que tu reproduiras si tu ajoutes un jour une nouvelle
   colonne basée sur un label.

5. **Lis `fct_oracle_neshu__conso_business_review`** — le modèle de fait le plus important ;
   il consolide 3 sources de données (télémétrie, chargement, livraison) en une seule
   table de consommation propre.

En cas de doute, pose la question ! Le graphe de lignage dbt (`dbt docs generate && dbt docs serve`)
montre l'arbre de dépendances complet de manière visuelle.
