

with commande_interne_base as (

    select
        -- Identifiants
        thp.idtask_has_product as task_product_id,
        t.idtask as task_id,
        t.idcompany_peer as company_id,
        thp.idproduct as product_id,
        t.idproduct_source as product_source_id,
        t.idproduct_destination as product_destination_id,
        t.type_product_source as product_source_type,
        t.type_product_destination as product_destination_type,

        -- Codes (source et destination)
        case 
            when t.type_product_source = 'COMPANY' then cs.code
            when t.type_product_source = 'RESOURCES' then rs.code
        end as source_code,
        case 
            when t.type_product_destination = 'COMPANY' then cd.code
            when t.type_product_destination = 'RESOURCES' then rd.code
        end as destination_code,

        p.code as product_code,
        ts.code as task_status_code,
        la.code as label_code,

        -- Informations date
        t.real_start_date as task_start_date,

        -- Métriques
        sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div) as quantity,
        sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div * thp.net_price) as valuation,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` thp 
        on thp.idtask = t.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` p 
        on p.idproduct = thp.idproduct
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_status` ts 
        on t.idtask_status = ts.idtask_status

    -- Source = COMPANY
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` cs 
        on t.idproduct_source = cs.idcompany 
       and t.type_product_source = 'COMPANY'

    -- Source = RESOURCES
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` rs 
        on t.idproduct_source = rs.idresources
       and t.type_product_source = 'RESOURCES'

    -- Destination = COMPANY
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` cd 
        on t.idproduct_destination = cd.idcompany 
       and t.type_product_destination = 'COMPANY'

    -- Destination = RESOURCES
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` rd 
        on t.idproduct_destination = rd.idresources
       and t.type_product_destination = 'RESOURCES'

    -- Jointures pour le filtrage sur labels télémétrie
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_task` lht
        on t.idtask = lht.idtask   
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` la
        on lht.idlabel = la.idlabel  
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` lf
        on la.idlabel_family = lf.idlabel_family

    where 1=1
        and t.idtask_status in (1, 4, 3)  -- FAIT, VALIDE, ANNULE
        and t.code_status_record = '1'
        and t.idtask_type = 132  -- LIVRAISON INTERNE
        and lf.code = 'STATUT_LIVRAISON'
        and t.real_start_date is not null

    group by
        thp.idtask_has_product, 
        t.idtask, 
        t.idcompany_peer,
        thp.idproduct, 
        thp.net_price,
        t.idproduct_source, 
        t.type_product_source,
        t.idproduct_destination, 
        t.type_product_destination,
        cs.code, rs.code, cd.code, rd.code,
        p.code, ts.code, la.code,
        t.real_start_date,
        t.updated_at, t.created_at, t.extracted_at
),

dedup as (
    select
        *,
        row_number() over (
            partition by task_product_id
            order by 
                case when label_code = 'LIVRE' then 1 else 2 end
        ) as rn
    from commande_interne_base
)

select
    -- Identifiants
    task_product_id,
    task_id,
    company_id,
    product_id,
    product_source_id,
    product_source_type,
    product_destination_id,
    product_destination_type,

    -- Codes
    source_code,
    destination_code,
    product_code,
    task_status_code,
    label_code,

    -- Infos métier
    task_start_date,

    -- Métriques
    quantity,
    valuation,

    -- Timestamps techniques
    updated_at,
    created_at,
    extracted_at

from dedup
where rn = 1
  and source_code is not null
  and destination_code is not null