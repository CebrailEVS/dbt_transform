

-- Alerte (warn) : produits dont le libellé évoque un conditionnement multiple
-- (rame, boîte, carton, distributeur de sucres, bambou...) mais qui sont absents
-- du seed ref_oracle_neshu__product_packaging. Ces produits sont aujourd'hui
-- comptés en unité (multiplicateur=1) dans fct_neshu__consommation et sont des
-- candidats à ajouter au seed après validation métier.
select
    p.product_id,
    p.product_name,
    p.product_type
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as p
left join `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__product_packaging` as pk
    on p.product_id = pk.product_id
where
    pk.product_id is null
    and regexp_contains(
        upper(p.product_name),
        r'(RAME( DE)? [0-9]+|BTE [0-9]+|CARTON DE [0-9]+|BATONNET [0-9]+|[0-9]+ SUCRES|BAMBOU INDI)'
    )