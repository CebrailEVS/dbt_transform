-- models/fct_chargement_quinzaine.sql


with base as (
    select
        p.product_type,
        cm.company_code,
        extract(year from cm.task_start_date) as annee_chgt,
        floor(
            date_diff(
                date(cm.task_start_date),
                date_trunc(
                    date_trunc(date(cm.task_start_date), year),
                    week (monday)
                ),
                day
            ) / 14
        ) + 1 as quinzaine_chgt,
        cm.load_quantity
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks` as cm
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` as p
        on cm.product_id = p.product_id
    where cm.task_start_date >= timestamp(
        datetime_sub(current_datetime(), interval 24 month)
    )
)

select
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt,
    sum(load_quantity) as quantite_chargee,
    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    '64a4dc76-772d-4d6a-b4b0-06492673b8bc' as dbt_invocation_id  -- noqa: TMP
from base
group by
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt
order by
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt