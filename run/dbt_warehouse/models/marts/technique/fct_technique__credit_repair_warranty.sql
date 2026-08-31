
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nAvoirs Repair Warranty \u00e0 d\u00e9duire de la facturation mensuelle NESPRESSO. Une\nd\u00e9cision RW \u00e9tablit qu'une intervention pass\u00e9e a \u00e9t\u00e9 mal faite (la panne est\nr\u00e9apparue) : EVS cr\u00e9dite alors Nespresso du montant factur\u00e9 sur cette\nintervention fautive. Correspond \u00e0 l'onglet \u00ab Repair Warranty \u00bb du classeur\nde facturation du manager technique, compt\u00e9 en n\u00e9gatif dans la grille.\n\u26a0\ufe0f \u00c0 ne pas confondre avec `fct_technique__repair`, qui qualifie les\nr\u00e9cidives de panne c\u00f4t\u00e9 NESHU/Yuman. Ici : p\u00e9rim\u00e8tre NESPRESSO, avoir de\nfacturation.\n\n[COMMENT CONSTRUITE]\n`stg_apptech__suivi_tech_rw` (d\u00e9cisions saisies dans l'app Suivi Tech)\nrejoint l'intervention FAUTIVE via `bad_intervention_id` = `n_planning` sur\n`int_nesp_tech__facturation_interventions` (montant, agence, machine) et\n`int_nesp_tech__interventions_dedup` (date de l'intervention fautive), plus\n`stg_yuman__users` pour le nom du technicien responsable. Le tarif cr\u00e9dit\u00e9\nest celui de la grille EN VIGUEUR \u00c0 LA DATE de l'intervention fautive (seed\nversionn\u00e9) : le cr\u00e9dit reproduit donc le montant factur\u00e9 \u00e0 l'\u00e9poque, sans\navoir \u00e0 le retrouver manuellement.\n\n[GRAIN]\n1 ligne par d\u00e9cision RW du mois. Cl\u00e9 composite\n(`periode_credit`, `intervention_fautive_id`).\n\n[NOTES]\n- Imputation au mois o\u00f9 la d\u00e9cision est **trait\u00e9e dans l'app**\n  (`periode_credit`), pas au mois de l'intervention fautive \u2014 d\u00e9cision\n  m\u00e9tier du 2026-08-05. L'ancien processus Excel imputait au mois d'origine :\n  l'\u00e9cart de m\u00e9thode est assum\u00e9, mais reste \u00e0 confirmer c\u00f4t\u00e9 comptabilit\u00e9.\n- **Fichiers de test d'ingestion exclus** : les d\u00e9p\u00f4ts\n  `RW_TEST_DATA_ING_<AAAA-MM>.xlsx` du DA portent de vraies interventions\n  fautives mais des correctives factices (`TESTDATAING-<AAAAMM>`). Sans\n  filtre, 2 avoirs de \u221290 \u20ac entraient dans la facturation. \u00c0 supprimer du\n  bucket c\u00f4t\u00e9 DA ; le filtre reste en d\u00e9fense.\n- Contrat du flux source, **applicatif et non port\u00e9 par une colonne** :\n  (1) d\u00e9cisions confirm\u00e9es uniquement \u2014 l'app \u00e9carte\n  `pole_expertise_RW != 'OUI'` (`app/domain/rw.py`) ; (2) p\u00e9rim\u00e8tre NESP\n  garanti \u2014 l'app r\u00e9sout les n\u00b0 saisis avec `src_inter='NESP'`\n  (`app/routers/rw.py:120`) et n'\u00e9met que les lignes retrouv\u00e9es. Ce second\n  point est ce qui autorise \u00e0 reconstruire `key_inter` par concat\u00e9nation :\n  789 `n_planning` NESP collisionnent avec un `intervention_id` YUMAN, un id\n  YUMAN qui entrerait ici cr\u00e9diterait la mauvaise intervention avec un\n  montant plausible, sans qu'aucun test ne le voie.\n- M\u00eame \u00e9v\u00e9nement m\u00e9tier, deux consommateurs ind\u00e9pendants : ce mart\n  (facturation) et le clawback de prime technicien (marts Primes, \u00e0 venir).\n- Si l'intervention fautive sort du p\u00e9rim\u00e8tre facturable (\u00e9tat `annul\u00e9e`,\n  agence `nespresso sud` filtr\u00e9e au d\u00e9dup, cl\u00e9 de facturation non r\u00e9solue),\n  c'est **tout le c\u00f4t\u00e9 droit** qui devient NULL \u2014 `agency`,\n  `categorie_machine`, `machine_clean`, `key_factu_fautive`,\n  `date_fin_fautive` et `montant_credit` : l'avoir perd ses coordonn\u00e9es dans\n  la grille. Base de risque : 12 277 des 87 113 interventions d\u00e9dupliqu\u00e9es\n  (14 %) sont hors du filtre d'\u00e9tat amont. `not_null` en `warn` sur\n  `montant_credit` et `agency` pour le signaler sans bloquer la prod.\n- `montant_credit` est **additif** : sommable par mois, agence, machine ou\n  technicien sans double compte, le grain \u00e9tant la d\u00e9cision.\n- Risque r\u00e9siduel de snapshot : le staging ne garde que le dernier NDJSON de\n  chaque p\u00e9riode (`max(extracted_at)`). Le brouillon RW \u00e9tant global au mois\n  c\u00f4t\u00e9 app (`rw_draft_key(annee, mois)`), une resoumission r\u00e9\u00e9met tout le\n  mois \u2014 mais un d\u00e9p\u00f4t partiel sur un mois d\u00e9j\u00e0 livr\u00e9 effacerait les avoirs\n  des autres agences, et le test de volume ne verrait pas une chute de 12 \u00e0 4.\n- D\u00e9calage observ\u00e9 : l'intervention fautive date du mois de la d\u00e9cision ou du\n  pr\u00e9c\u00e9dent (avoirs de mai 2026 : fautives du 24/04 au 20/05). Le \u00ab M-2 \u00bb du\n  cadrage DA est le d\u00e9calage entre la d\u00e9cision et la **facture** qui la\n  reprend, pas entre l'intervention et la d\u00e9cision \u2014 l'onglet RW du classeur\n  de juillet porte les d\u00e9cisions de mai. Cons\u00e9quence : cet avoir de mai\n  atterrit ici sur `periode_credit` = mai, l\u00e0 o\u00f9 l'Excel le portait sur la\n  facture de juillet. \u00c9cart de m\u00e9thode \u00e0 chiffrer lors du rapprochement de\n  l'\u00e9tape 4.\n"""
    )
    as (
      

-- Avoirs Repair Warranty à déduire de la facturation mensuelle NESPRESSO.
--
-- Une décision RW dit qu'une intervention passée a été MAL FAITE (la panne est
-- réapparue) : EVS crédite alors Nespresso du montant qui avait été facturé sur
-- cette intervention fautive. C'est l'onglet « Repair Warranty » du classeur du
-- manager, compté en négatif dans la grille de facturation.
--
-- Grain : 1 ligne par décision RW du mois. Le montant est porté par
-- l'intervention FAUTIVE (celle qui est créditée), pas par l'intervention
-- corrective qui l'a suivie.
--
-- Imputation : au mois où la décision est traitée dans l'app (`periode_date`),
-- et non au mois de l'intervention fautive — décision métier du 2026-08-05,
-- l'écart de méthode avec l'ancien processus Excel est assumé.
--
-- Contrat du flux source : `stg_apptech__suivi_tech_rw` ne contient QUE des
-- décisions confirmées. L'app filtre `pole_expertise_RW != 'OUI'` avant écriture
-- (`app/domain/rw.py`) et n'émet que les lignes retrouvées en BigQuery. Ce
-- contrat est applicatif, pas porté par une colonne : le test de volume du YAML
-- sert de garde-fou si l'app changeait ce filtre.
--
-- Ce mart est le consommateur « facturation » du flux RW. Le clawback de prime
-- technicien en est l'autre consommateur, indépendant (marts Primes, à venir) :
-- même événement métier, deux conséquences financières distinctes.
--
-- ⚠️ À ne pas confondre avec `fct_technique__repair`, qui qualifie les récidives
-- de panne côté NESHU/Yuman. Ici : périmètre NESPRESSO, avoir de facturation.

with decisions_rw as (

    select
        periode_date,
        cast(bad_intervention_id as string) as bad_intervention_id,
        cast(new_intervention_id as string) as new_intervention_id,
        tech_id as technician_id,
        extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_rw`
    -- Les fichiers de test d'ingestion du DA portent de vraies interventions
    -- fautives mais des correctives factices (TESTDATAING-<AAAAMM>) : sans ce
    -- filtre, 2 avoirs de -90 € entreraient dans la facturation NESPRESSO.
    -- Le test `relationships` sur key_inter_corrective attrape tout autre cas.
    where not starts_with(upper(coalesce(source, '')), 'RW_TEST_DATA_ING')

),

-- Intervention fautive : montant crédité, agence et machine à imputer.
-- Le tarif vient de la grille EN VIGUEUR À LA DATE de cette intervention (seed
-- versionné) : le crédit reproduit donc ce qui avait été facturé, sans avoir à
-- retrouver le montant historique.
intervention_fautive as (

    select
        cast(factu.n_planning as string) as n_planning,
        factu.agency,
        factu.categorie_machine,
        factu.machine_clean,
        factu.key_factu,
        factu.tarif_factu,
        date(dedup.date_heure_fin, 'Europe/Paris') as date_fin
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__facturation_interventions` as factu
    inner join `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup` as dedup
        on factu.n_planning = dedup.n_planning

)

select
    -- Grain : mois d'imputation + intervention fautive créditée
    date_trunc(rw.periode_date, month) as periode_credit,
    rw.bad_intervention_id as intervention_fautive_id,

    -- FK
    concat('NESP_', rw.bad_intervention_id) as key_inter_fautive,
    rw.technician_id,

    -- Identité lisible de la corrective (audit). Elle-même facturée normalement,
    -- et rien n'interdit qu'elle soit à son tour créditée si elle récidive :
    -- 1 cas sur 14 aujourd'hui (chaînage récidive → récidive).
    rw.new_intervention_id as intervention_corrective_id,
    concat('NESP_', rw.new_intervention_id) as key_inter_corrective,

    -- Attributs de l'intervention fautive, dimensions de la grille de facturation
    fautive.agency,
    tech.user_name as technician_name,
    fautive.categorie_machine,
    fautive.machine_clean,
    fautive.key_factu as key_factu_fautive,

    -- Date de l'intervention fautive : mesure le décalage avec le mois d'imputation
    fautive.date_fin as date_fin_fautive,

    -- Mesure : toujours négative, c'est un avoir
    -1 * fautive.tarif_factu as montant_credit,

    -- Métadonnées
    rw.extracted_at as rw_extracted_at

from decisions_rw as rw
left join intervention_fautive as fautive
    on rw.bad_intervention_id = fautive.n_planning
-- Nom résolu sur la DIM et non sur le staging Yuman : la FK est testée contre la
-- dim, et 31 des 97 utilisateurs du staging n'y sont pas (elle ne garde que les
-- techniciens et les managers-techniciens). Résoudre sur le staging afficherait
-- un nom là où la FK est orpheline.
left join `evs-datastack-prod`.`prod_marts`.`dim_technique__technician` as tech
    on rw.technician_id = tech.user_id
    );
  