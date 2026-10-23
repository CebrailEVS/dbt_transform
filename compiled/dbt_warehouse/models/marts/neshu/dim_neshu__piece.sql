

with pieces as (
    select *
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product`
    where
        idproduct_type = 2 -- PIECE : pièces détachées
        and code_status_record <> -1 -- exclut les ghost-deletes ERP
),

-- Seul label porté par les pièces.
isactive as (
    select
        lhp.idproduct,
        max(la.code) as isactive
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_product` as lhp
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as la
        on lhp.idlabel = la.idlabel
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on la.idlabel_family = lf.idlabel_family
    where lf.code = 'ISACTIVE'
    group by lhp.idproduct
),

suppliers as (
    select
        idcompany,
        name
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company`
)

select
    p.idproduct as piece_id,
    p.idcompany_supplier as supplier_company_id,
    p.code as piece_code,
    p.name as piece_name,
    nullif(trim(p.commercial_name), '') as piece_commercial_name,
    s.name as supplier_name,
    coalesce(lower(a.isactive) = 'yes', false) as is_active,
    p.purchase_unit_price,
    timestamp(datetime(p.created_at, 'Europe/Paris')) as created_at,
    timestamp(datetime(p.updated_at, 'Europe/Paris')) as updated_at

from pieces as p
left join isactive as a
    on p.idproduct = a.idproduct
left join suppliers as s
    on p.idcompany_supplier = s.idcompany