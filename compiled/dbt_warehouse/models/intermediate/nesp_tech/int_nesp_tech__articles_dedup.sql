

with ranked as (

    select *
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__articles`

    qualify ROW_NUMBER() over (
        partition by n_planning, code_article
        order by extracted_at desc
    ) = 1

)

select * from ranked