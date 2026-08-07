# Chantier technique / apptech — état du chantier

**Fichier d'état partagé DE ↔ DA.** Une seule source de vérité sur « où on en
est ». Se met à jour **en place** (on réécrit les lignes, l'historique est dans
git) — pas de nouveau document daté par échange.

Périmètre couvert : app Suivi Tech (`evs-suivi-tech`) → flux NDJSON GCS →
staging/intermediate apptech → marts technique (facturation retraitée, primes,
facturation NESPRESSO).

Dernière mise à jour : 2026-08-07 (DE).

---

## 1. Rôles

| Qui | Décide / possède |
|---|---|
| **Cebrail (Data Ing)** | modèles dbt (staging → marts), seeds, tables externes, infra/orchestration |
| **Etienne (Data Analyst)** | app Suivi Tech (pages, flux NDJSON émis), captation des règles métier, restitution |
| **Métier (manager technique)** | règles de facturation et de prime, arbitrages de périmètre |

Contrat d'identité des flux : `docs/apptech_ingestion.md` § 1.

---

## 2. En production

| Objet | Depuis |
|---|---|
| 8 flux staging `stg_apptech__suivi_tech_*` (6 retraitements + `events` + `rw`) | PR #178 / #180 |
| `int_apptech__retraitements` (6 flux, grain `key_inter × type`) | PR #186, 2026-07 |
| `fct_technique__intervention_retraitee` (colonnes `_effectif`) | PR #197, validé prod 2026-08-04 |
| `fct_technique__repair` (qualification repair NESHU, épisodes) | PR #183 — consommé par la page RW NESHU de l'app |

Validation prod du 2026-08-04 : 93 897 lignes, 118 retraitées, 0 orphelin,
0 dérive, 0 collision. Détail : `.claude/notes/retraitement_technique/rapport_final_DA_mart_retraitee.md`.

---

## 3. Reste à faire

| Item | Qui | Dépend de | Statut |
|---|---|---|---|
| Rebuild auto de la chaîne apptech (Cloud Run Job du bouton « Build », ou build planifié `tag:apptech`) | DE | — | à faire — **bloque la fraîcheur prod** |
| Snapshot daté du mart retraitée (append + `build_ts`) | DE | — | décidé, non codé |
| Propager `agency` (Nomad) dans `int_nesp_tech__facturation_interventions` → fait → mart. **Filtrer `nespresso sud`** (sous-traitant hors périmètre) | DE | — | à faire — **prérequis facturation NESPRESSO** |
| Seed `ref_nesp_tech__key_facturation` versionné `valid_from`/`valid_to` (patron `ref_yuman__tarification_clean`) + 2 variantes Montagne manquantes (`Enlevement` Zenius/Gemini) | DE | — | à faire |
| `decision_rw` explicite dans le flux `/rw` (aucun champ de décision aujourd'hui) | **DA (app)** | — | à demander — **bloque le crédit RW** |
| `int_nesp_tech__facturation_rw_credits` (crédit négatif) | DE | `decision_rw` | bloqué |
| Bonus facturation règle Excel + coupe-circuit agence (modèle intermediate dédié pour le taux RW, sinon cycle dbt) | DE | crédit RW | à faire ensuite |
| Vue `fct_technique__facturation_nespresso_mensuelle` (grain agence × famille × type) | DE | les 4 lignes ci-dessus | à faire |
| Ingestion du flux `rw_neshu` (table externe + staging) | DE | libellés figés côté app | en attente DA |
| Ingestion du flux `prime_get` (2 fichiers de test sur GCS, jamais ingéré) | DE | usage à trancher | non tranché |
| Lecture de `suivi_tech/config/technicians_overrides.json` (exclusion primes) | DE | design mart Primes | non tranché |
| Marts **Primes** (Technicien / GET / Manager Tech) | DE | règles reçues (3 docs `POINT_DATA_ING`) | à concevoir |
| Broutilles : coquille doc `types_retraitement` (séparateur `,`), tag `apptech` absent sur intermediate + mart | DE | — | à faire au passage |

---

## 4. Décisions gelées

Ne pas rouvrir sans raison nouvelle. Source = document de décision d'origine.

| Décision | Date | Source |
|---|---|---|
| Identité : `key_inter = src_inter_'_'_intervention_id` ; `intervention_id` = `workorder_id` (YUMAN) / `n_planning` (NESP) ; `numero_pu` = n° affiché | 2026-07-23, validé end-to-end 07-24 | `contrat_ndjson_identite.md` |
| Colonnes sans suffixe = brut du fait (audit) ; `_effectif` = post-retraitement | 2026-07-24 | mart retraitée |
| `intervention_id = CAST(workorder_id AS STRING)` est un **invariant garanti** pour `src_inter = 'YUMAN'` | 2026-08-07 | `fct_technique__intervention.sql:68` |
| Période de facturation = `DATE(date_fin, Europe/Paris)` | 2026-08-05 | facturation § 5.7 |
| Détection mini-prev = article posé (méthode dbt fait foi, pas la recherche texte Excel) | 2026-08-05 | facturation § 5.3 |
| MEE = flag `a_facturer` + tarif standard (pas de montant libre par ligne) | 2026-08-05 | facturation § 5.2 |
| Bonus facturation = règle Excel (SLA ≤ J+1, Transportable/Momento, coupe-circuit agence 2,5 % RW / 3 mois), en **colonnes dédiées** — `bonus_bool` inchangé | 2026-08-05/06 | facturation § 5.1 |
| Crédit RW imputé au mois de la **décision**, montant recalculé au tarif courant | 2026-08-05 | facturation § 5.6 — sous réserve compta (§ 5) |
| RW = 1 flux source, 2 consommateurs indépendants (crédit facturation + clawback prime) | 2026-08-06 | facturation § 5.11 |
| **Agence de facturation = `agency` (Nomad)**, pas `tech_secteur`. Les 4 valeurs (`evs`, `evs idf`, `evs paris`, `evs paris 2`) correspondent aux 4 libellés Excel — le TCD Excel est bâti sur l'export Nomad, c'est la même donnée. `tech_secteur` (5 valeurs, dont `evs est` absent de la grille) recouvre plusieurs agences et ne peut pas servir de dimension de facturation | 2026-08-07 | mesuré sur juillet 2026, 1 956 interventions |
| Mini-prev et Enlèvement : tarifs **déjà corrects**, rien à construire | 2026-08-06 | facturation § 5.3 / 5.8 |
| Hors périmètre : majoration T3 Dispenser, barème de primes de l'onglet Excel `Paramètres` | 2026-08-05/06 | facturation § 5.4 / 5.10 |
| Prime astreinte : forfait jour (50/100 €), une ligne = un weekend (150/200 €), sauf moitié à cheval sur 2 mois ; fériés = `ref_general__feries_metropole` | 2026-07-31 | `POINT_DATA_ING_2026-07-31` |
| Réaffectation astreinte : transfert **symétrique** de prod (retrait au fictif, crédit au réel), VALIDATED seulement | 2026-07-31 | idem § 6 |
| Prime Paris : 10 €/jour avec ≥ 1 intervention Paris, plafond 200 €/mois ; objectif modulé 8 / 6 / taux flat | 2026-08-03/04 | `POINT_DATA_ING_2026-08-03` |

---

## 5. Questions ouvertes

Une ligne = une question, avec un destinataire nommé. Pas de question noyée
dans un paragraphe.

| Question | Pour | Statut |
|---|---|---|
| Le libellé « **EVS AURA** » de la grille Excel correspond-il bien à `agency = 'evs'`, qui contient aussi l'activité du secteur **EST** (juillet 2026 : 297 interventions, 50 234 €) ? Si le métier veut un jour EST en ligne séparée, la donnée le permet (`tech_secteur`), la grille Excel non | métier | ouverte — n'empêche pas de coder |
| Ajouter `decision_rw` (OUI/NON) au flux `/rw` ? Aujourd'hui aucun champ de décision → un crédit financier reposerait sur une hypothèse implicite | DA (app) | ouverte |
| La compta / Sage attend-elle l'imputation du crédit RW au mois d'**origine** ? Si oui, la décision § 5.6 est à revoir | métier | ouverte |
| Mapping MEE / modif_intervention → OUI/NON : à valider sur un mois réel complet (échantillon bêta jusqu'ici) avant facturation réelle | DA | ouverte |
| `rw_neshu` : libellés et options de décision à figer avant ingestion | DA | ouverte |
| Marts Primes — colonnes séparées vs total fusionné ; traitement des anomalies de saisie astreinte ; retrait/crédit net ; stabilité de `flag_paris_intramuros` pour un mart figé | DE + DA | à trancher au design |

Questions **fermées** au 2026-08-07 : invariant `workorder_id` (garanti, cf. § 4) ·
`delai_bonus_bool` consommé ailleurs ? (non — aucun mart Primes n'existe encore) ·
`stg_apptech__suivi_tech_rw` a-t-il un consommateur exclusif ? (non — zéro
consommateur aval aujourd'hui) · `src_inter`/`numero_pu` lus en staging ? (oui,
sur les 6 flux depuis PR #186 — la mention contraire dans `PROCHAINES_ETAPES.md`
étape 5 est obsolète).

---

## 6. Règles de travail (anti-noyade)

Adoptées le 2026-08-07 après un cycle où les deux côtés ont produit plus de
documentation que l'autre ne pouvait en lire.

1. **Ce fichier porte l'état.** Les `POINT_DATA_ING_*` restent des relevés de
   **décisions de règles métier** (précieux, on les garde) — ils ne décrivent
   plus l'avancement.
2. **Pas d'historique de correction dans un document.** On réécrit la ligne ;
   git garde la trace. Un doc qui contient « correctif du J+1 » trois fois
   coûte plus à lire qu'à réécrire.
3. **`git pull` avant toute analyse de l'existant.** Un clone en retard de
   18 commits a coûté une passe entière d'analyse le 2026-08-05.
4. **Une spec = 1 page + tableaux.** Au-delà, le détail passe en annexe.
5. **Une question ouverte = une ligne du § 5**, avec un destinataire. Une
   question dans un paragraphe n'existe pas.
