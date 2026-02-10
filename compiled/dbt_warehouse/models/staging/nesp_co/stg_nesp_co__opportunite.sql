

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`nespresso_commerce_opportunite`

),

base_opportunite as (

    select
        -- ids convertis en bigint
        cast(
            case
                when opportunity = '#' then null
                when opportunity = 'Result' then null
                else opportunity
            end as int64
        ) as opportunity_id,

        -- id string
        nullif(unnamed_2, '#') as c4c_id_commercial,
        cast(regexp_replace(nullif(unnamed_13, '#'), r'\.0$', '') as int64) as c4c_id_account,
        nullif(nessoft_id, '#') as nessoft_id_account,
        cast(nullif(unnamed_8, '#') as int64) as c4c_id_campaign,

        -- colonnes texte
        nullif(employee_responsible, '#') as employee_responsible,
        nullif(created_by, '#') as created_by,
        nullif(unnamed_6, '#') as opportunity_name,
        nullif(campaign, '#') as campaign_name,
        nullif(source, '#') as source_name,
        nullif(account, '#') as account_name,
        nullif(reason_for_status, '#') as reason_for_status,
        nullif(sales_unit, '#') as sales_unit,
        nullif(role_account, '#') as role_account,
        nullif(lifecycle_status, '#') as lifecycle_status,

        -- mesures numériques
        cast(nullif(first_coffee_order, '#') as float64) as first_coffee_order,
        cast(nullif(expected_value, '#') as float64) as expected_value,
        cast(nullif(zeq_zenius_equivalent, '#') as float64) as zeq_zenius_equivalent,
        cast(
            replace(
                replace(
                    trim(nullif(chance_of_success, '#')),
                    '%',
                    ''
                ),
                ',',
                '.'
            ) as float64
        ) as chance_of_success,
        cast(nullif(machines, '#') as int64) as machines_opportunity,

        -- dates harmonisées (converties en timestamp)
        timestamp(nullif(created_on, '#')) as created_on,
        timestamp(nullif(close_date, '#')) as close_date,

        -- metadata
        timestamp(extracted_at) as extracted_at,
        timestamp(file_date) as file_date,
        source_file

    from source_data

)

select *
from base_opportunite
where opportunity_id is not null