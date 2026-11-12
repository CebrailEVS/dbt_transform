# Couche `exports` — fichiers livrés à un destinataire

Règles transversales : [CONVENTIONS.md](../../CONVENTIONS.md).

## 1. Rôle

Un export est un fichier convenu avec un destinataire et livré à date fixe par un job (mail,
dépôt de fichier). La couche `exports` porte le **contrat** de ce fichier : ses colonnes, ses
filtres et ses règles de calcul, versionnés et testés à un seul endroit.

| | Mart | App | Export |
|---|---|---|---|
| Lu par | Power BI | une application interactive | un job planifié qui produit un fichier |
| Forme | étoile (faits + dimensions) | taillée pour les écrans | le fichier, colonne pour colonne |
| Rangé par | BU | application | destinataire |
| Dataset | `prod_marts` | `prod_app_<application>` | `prod_export` |

## 2. Contenu d'un modèle

- **Un modèle = un fichier.** Un fichier qui regroupe plusieurs sources se compose dans le job,
  pas dans dbt : un modèle par source, car les workflows des sources tournent séparément.
- **Les colonnes suivent l'ordre du fichier**, avec des noms techniques et des types propres
  (`DATE`, `NUMERIC`). Les colonnes constantes ou vides du format sont dans le modèle : ouvrir la
  table, c'est voir le fichier.
- **La clé technique vient en dernier.** Elle sert à tester le grain et n'est pas envoyée.
- **Tout l'historique.** Le job choisit la période en filtrant une colonne de date : n'importe
  quel mois se rejoue.
- **La présentation reste dans le job** : en-têtes, séparateur, format des dates et des nombres,
  encodage.

## 3. Lecture

- **Le staging par défaut.** Un changement fait pour Power BI dans un intermediate ou un mart ne
  doit pas modifier un fichier déjà convenu.
- **L'intermediate par exception**, quand la logique y existe et serait trop lourde à dupliquer.
  La description dit pourquoi.
- **Jamais un mart ni une app.** La règle `checks/export_lit_un_mart.sql` le vérifie.
- Un export qui lit le staging Oracle réapplique les règles de la source
  ([architecture](../architecture/oracle_neshu.md)) : type de tâche, enregistrement actif, statut,
  jour en heure de Paris, libellés filtrés sur `fr_FR`.

## 4. Nommage

| Élément | Règle | Exemple |
|---|---|---|
| Dossier | `models/exports/<destinataire>/` | `models/exports/finance/` |
| Modèle | `exp_<destinataire>__<source>_<entité>` | `exp_finance__neshu_reception` |
| YAML | `_<destinataire>__export_models.yml` | `_finance__export_models.yml` |
| Exposure | `models/exposures/<destinataire>.yml`, `type: application` | `export_receptions_distrilog` |

## 5. Matérialisation

`table`, explicite dans le `config()`. Le modèle descend de `source:<src>+` : le workflow de sa
source le reconstruit, sans orchestration dédiée. Pas de partition : une table d'export reste petite.

## 6. Documentation et tests

- Description en 4 blocs, comme les marts ([marts.md § 2](marts.md#2-description-en-4-blocs)).
  `[NOTES]` rappelle que le modèle est réservé au job d'export.
- Chaque colonne cite l'en-tête du fichier qu'elle alimente (« Montant HT »…).
- Clé technique `unique` + `not_null` ; `not_null` sur les colonnes obligatoires du fichier.
- **Les tests conditionnent l'envoi.** Le job n'envoie le fichier que si tous les tests de la
  table ont réussi dans le build `dbt-runner` qui l'a produite : un test en `error` bloque l'envoi,
  un test en `warn` le laisse partir avec une alerte. Une règle de qualité du fichier s'écrit donc
  ici, en test, jamais dans le job.
- Chaque test porte le nom du modèle : le job reconnaît les tests de sa table à ce nom. Un test
  singulier se nomme `<modèle>_<règle>.sql`. La règle `checks/export_test_sans_nom_du_modele.sql`
  le vérifie.
- Exposure obligatoire : elle nomme le job, la cadence et le destinataire. `dbt ls -s
  +exposure:<export>` donne l'impact d'un changement.

## 7. Checklist PR

- [ ] modèle dans `models/exports/<destinataire>/`, préfixe `exp_<destinataire>__`, `table` explicite
- [ ] colonnes dans l'ordre du fichier, clé technique en dernier
- [ ] YAML 4 blocs, en-tête du fichier dans chaque description de colonne, test du grain
- [ ] chaque règle de qualité du fichier est un test dbt nommé d'après le modèle
- [ ] exposure à jour
- [ ] `dbt lint` vert, build dev vert, rapprochement avec un fichier de référence
