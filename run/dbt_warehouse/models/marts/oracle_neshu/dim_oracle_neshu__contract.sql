
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__contract`
      
    
    cluster by contract_id

    
    OPTIONS(
      description="""Dimension contrat : pivot des labels et s\u00e9lection du contrat actif principal par client.\nCette table contient un seul contrat actif par client (company_id), s\u00e9lectionn\u00e9 selon\nla date de fin de contrat la plus r\u00e9cente, puis la date de d\u00e9but la plus r\u00e9cente.\n"""
    )
    as (
      

with contract_labels as (
    select
        c.idcontract as contract_id,
        c.idcompany_peer as company_id,
        c.code as contract_code,
        c.engagement_raw,
        c.engagement_clean,
        c.nombre_collab,
        l.code as label_code,
        lf.code as label_family_code,
        c.original_start_date,
        c.original_end_date,
        c.current_end_date,
        c.termination_date,
        c.created_at,
        c.updated_at
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__contract` as c
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_contract` as lhc
        on c.idcontract = lhc.idcontract and lhc.idlabel is not null
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lhc.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
),

aggregated_labels as (
    select
        contract_id,
        company_id,
        contract_code,
        engagement_raw,
        engagement_clean,
        nombre_collab,
        original_start_date,
        original_end_date,
        current_end_date,
        termination_date,
        created_at,
        updated_at,

        -- pivot des familles de labels
        MAX(case when label_family_code = 'TRANCHE_COLLAB' then label_code end) as employee_range,
        MAX(case when label_family_code = 'PROADMAN' then label_code end) as proadman,
        MAX(case when label_family_code = 'REGION' then label_code end) as region,
        MAX(case when label_family_code = 'TELETRAVAIL' then label_code end) as teletravail,
        MAX(case when label_family_code = 'ISACTIVE' then label_code end) as is_active

    from contract_labels
    group by
        contract_id,
        company_id,
        contract_code,
        engagement_raw,
        engagement_clean,
        nombre_collab,
        original_start_date,
        original_end_date,
        current_end_date,
        termination_date,
        created_at,
        updated_at
),

aggreated_contract as (
    select
        contract_id,
        company_id,
        contract_code,
        engagement_raw,
        engagement_clean,
        nombre_collab,
        COALESCE(LOWER(is_active) = 'yes', false) as is_active,
        original_start_date,
        original_end_date,
        current_end_date,
        termination_date,
        created_at,
        updated_at
    from aggregated_labels
)

select
    contract_id,
    company_id,
    contract_code,
    engagement_raw,
    engagement_clean,
    nombre_collab,
    is_active,
    original_start_date,
    original_end_date,
    current_end_date,
    termination_date,
    created_at,
    updated_at
from (
    select
        *,
        ROW_NUMBER() over (
            partition by company_id
            order by current_end_date desc, original_start_date desc, contract_id asc
        ) as rn
    from aggreated_contract
    where is_active = true
) as subq
where rn = 1
    );
  