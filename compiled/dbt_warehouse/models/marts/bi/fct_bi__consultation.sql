

with parc as (
    select
        report_id,
        workspace_id,
        dataset_id,
        report_name,
        workspace_name
    from `evs-datastack-prod`.`prod_marts`.`dim_bi__rapport`
),

consultations as (
    select
        event_id,
        report_id,
        user_id,
        created_at,
        consumption_method,
        distribution_method
    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__events`
    where activity = 'ViewReport'
)

select
    -- grain : 1 ligne = 1 consultation d'un rapport par une personne
    date(c.created_at) as consultation_date,
    c.event_id,

    -- cles etrangeres
    c.report_id,
    p.workspace_id,
    p.dataset_id,

    -- attributs d'affichage aplatis du parent direct (marts.md § 1)
    p.report_name,
    p.workspace_name,

    -- QUI : dimension degeneree portee par le fait. 32 lecteurs distincts pour
    -- 2 domaines, ce qui ne justifie pas une dim_bi__utilisateur dediee.
    -- Donnee personnelle : voir l'en-tete de _bi__marts_models.yml.
    c.user_id,
    split(c.user_id, '@')[safe_offset(1)] as user_domain,

    -- COMMENT
    c.consumption_method,
    c.distribution_method,

    -- horodatage precis de la consultation
    c.created_at as consultation_at

from consultations as c
-- INNER JOIN : restreint aux consultations du parc metier. Ecarte les 2 vues
-- portant sur un rapport hors perimetre (copie d'App ou metriques d'usage).
inner join parc as p on c.report_id = p.report_id