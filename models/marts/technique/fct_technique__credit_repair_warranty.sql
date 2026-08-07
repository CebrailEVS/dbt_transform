{{ config(materialized='table') }}

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
    from {{ ref('stg_apptech__suivi_tech_rw') }}
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
    from {{ ref('int_nesp_tech__facturation_interventions') }} as factu
    inner join {{ ref('int_nesp_tech__interventions_dedup') }} as dedup
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
left join {{ ref('dim_technique__technician') }} as tech
    on rw.technician_id = tech.user_id
