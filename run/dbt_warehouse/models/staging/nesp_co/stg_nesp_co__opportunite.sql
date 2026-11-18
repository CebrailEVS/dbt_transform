
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__opportunite`
      
    partition by file_date
    

    
    OPTIONS(
      description="""Opportunit\u00e9s commerciales Nespresso (CRM C4C), typ\u00e9es depuis `nesp_co_opportunite`, hors lignes de sous-total `Result`. Le raw empile un export complet par journ\u00e9e : le staging garde, pour chaque opportunit\u00e9, la version de sa derni\u00e8re journ\u00e9e (`file_date`). Cl\u00e9 : `opportunity_id`. Certains exports laissent `role_account` et `zeq_zenius_equivalent` vides sur toutes leurs lignes : leur version n'est gard\u00e9e que pour une opportunit\u00e9 qu'aucun autre export ne porte.\n"""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`nesp_co_opportunite`

),

base_opportunite as (

    select
        -- ids
        cast(
            case
                when opportunity = '#' then null
                when opportunity = 'Result' then null
                else opportunity
            end as int64
        ) as opportunity_id,
        nullif(unnamed_2, '#') as c4c_id_commercial,
        cast(regexp_replace(nullif(unnamed_13, '#'), r'\.0$', '') as int64) as c4c_id_account,
        nullif(nessoft_id, '#') as nessoft_id_account,
        cast(nullif(unnamed_8, '#') as int64) as c4c_id_campaign,

        -- texte
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

        -- mesures
        cast(nullif(first_coffee_order, '#') as float64) as first_coffee_order,
        cast(nullif(expected_value, '#') as float64) as expected_value,
        cast(nullif(zeq_zenius_equivalent, '#') as float64) as zeq_zenius_equivalent,
        cast(
            replace(replace(trim(nullif(chance_of_success, '#')), '%', ''), ',', '.')
            as float64
        ) as chance_of_success,
        cast(nullif(machines, '#') as int64) as machines_opportunity,

        -- dates
        timestamp(nullif(created_on, '#')) as created_on,
        timestamp(nullif(close_date, '#')) as close_date,

        -- metadata
        _extracted_at as extracted_at,
        snapshot_date as file_date,
        _fichier as source_file,

        -- Certains exports laissent role_account et zeq_zenius_equivalent vides sur toutes leurs
        -- lignes : leur version ne sert que si aucun autre export ne porte l'opportunité.
        (
            countif(role_account != '#') over (partition by snapshot_date) > 0
            and countif(zeq_zenius_equivalent != '#') over (partition by snapshot_date) > 0
        ) as has_role_et_ez

    from source_data
    where opportunity != 'Result' or opportunity is null

),

-- Le raw empile un export par journée : on garde la version de la dernière journée, en
-- préférant un export qui porte le rôle et l'EZ. Tri sur la journée : un rattrapage charge
-- plusieurs journées sous un même extracted_at.
deduped as (

    select * except (has_role_et_ez)
    from base_opportunite
    where opportunity_id is not null
    qualify row_number() over (
        partition by opportunity_id
        order by has_role_et_ez desc, file_date desc, extracted_at desc
    ) = 1

)

select * from deduped
    );
  