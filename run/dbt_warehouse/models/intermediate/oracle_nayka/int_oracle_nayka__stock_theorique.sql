
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_nayka__stock_theorique`
      
    partition by snapshot_date
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Stock th\u00e9orique journalier des produits Nayka, par d\u00e9p\u00f4t, pr\u00e9par\u00e9 pour l'application Cockpit Supply.\n[COMMENT CONSTRUITE] Issu de stg_oracle_nayka_gcs__stock_theorique, sans filtre de d\u00e9p\u00f4t. depot_role vient du code du d\u00e9p\u00f4t (macro nayka_depot_role, m\u00eame r\u00e8gle que les \u00e9quipements). product_type vient de la fiche article, jointe par le code. is_out_of_stock d\u00e9riv\u00e9 de stock_at_date = 0. dpa repli sur purchase_price si manquant.\n[GRAIN] 1 ligne par (snapshot_date, entity_type, id_entity, product_code).\n[NOTES] Les mod\u00e8les de machines (product_type = MODELE) y figurent en quantit\u00e9 alors que chaque machine est aussi un \u00e9quipement de int_oracle_nayka__device : ne jamais additionner les deux. entity_type fait partie du grain : idcompany et idresources viennent de deux s\u00e9quences Oracle distinctes. plus/moins = cumuls d'entr\u00e9es/sorties depuis le dernier inventaire physique. date_system porte l'heure du batch ; filtrer sur snapshot_date.\n"""
    )
    as (
      

-- La photo ne porte que le code article, et un même code peut désigner des fiches de types
-- différents : il reste alors sans type, plutôt que d'en recevoir un au hasard.
with produit as (
    select
        p.code as product_code,
        if(count(distinct pt.code) = 1, any_value(pt.code), null) as product_type
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__product` as p
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__product_type` as pt
        on p.idproduct_type = pt.idproduct_type
    where coalesce(p.code_status_record, 0) != -1
    group by p.code
)

select
    st.snapshot_date,
    st.id_entity,
    st.entity_type,
    st.product_code,
    st.resources_code as entity_code,
    st.entity_name,
    case when st.entity_type = 'company' then case
    when st.resources_code = 'DEPOTREBUT' then 'rebut'
    when st.resources_code = 'MARKETING' then 'hors_stock'
    else 'stock'
end end as depot_role,
    st.product_name,
    pr.product_type,
    st.stock_at_date,
    st.stock_at_date = 0 as is_out_of_stock,
    st.date_inventaire,
    st.stock_inventaire,
    st.plus,
    st.moins,
    coalesce(st.dpa, st.purchase_price) as dpa,
    st.purchase_price,
    st.date_system,
    st.extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka_gcs__stock_theorique` as st
left join produit as pr
    on st.product_code = pr.product_code
    );
  