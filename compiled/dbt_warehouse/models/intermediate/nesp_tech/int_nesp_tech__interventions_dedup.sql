
-- Liste des interventions dédupliquées par la date de fin
with ranked as (

    select *
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__interventions`

    qualify ROW_NUMBER() over (
        partition by n_planning
        order by date_heure_fin desc, extracted_at desc
    ) = 1

)

select * from ranked