

with base as (
    select
        (case
            when p.product_type in ('BOISSONS FRAICHES', 'SNACKING') then 'SODA + SNACKS'
            else p.product_type
        end) as product_type,
        cm.company_code,
        comp.company_name,
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
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as p
        on cm.product_id = p.product_id
    inner join `evs-datastack-prod`.`prod_marts`.`dim_neshu__device` as d
        on
            cm.device_id = d.device_id
            and d.device_economic_model = 'Gratuit'
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__company` as comp
        on cm.company_id = comp.company_id
    where
        cm.task_start_date >= timestamp_sub(current_timestamp(), interval 730 day)
        and cm.task_status_code in ('FAIT', 'VALIDE')
)

select
    product_type,
    company_code,
    company_name,
    annee_chgt,
    quinzaine_chgt,
    sum(load_quantity) as quantite_chargee,

    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    'a19789c5-ea7f-4f3a-9564-a8dc9ccb12f2' as dbt_invocation_id  -- noqa: TMP
from base
group by
    product_type,
    company_code,
    company_name,
    annee_chgt,
    quinzaine_chgt