
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__stock_theorique`
      
    partition by snapshot_date
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Stock th\u00e9orique journalier des produits Neshu, par v\u00e9hicule roadman et par d\u00e9p\u00f4t produits, pr\u00e9par\u00e9 pour l'application Cockpit Supply ind\u00e9pendamment des marts.\n[COMMENT CONSTRUITE] Issu de stg_oracle_neshu_gcs__stock_theorique. D\u00e9p\u00f4ts retenus : 01 \u00e0 05, 10 et 13 (entity_type = 'company') ; ressources de type VEHICLE hors ghost-deletes (entity_type = 'resource'), ce qui \u00e9carte la PERSON pr\u00e9sente dans le stock. is_vehicle_active vient du label ISACTIVE de stg_oracle_neshu__resources (\u00e9tat courant, non filtrant), NULL pour les d\u00e9p\u00f4ts. is_out_of_stock d\u00e9riv\u00e9 de stock_at_date = 0. dpa repli sur purchase_price si manquant.\n[GRAIN] 1 ligne par (snapshot_date, entity_type, id_entity, product_code).\n[NOTES] M\u00eame logique que fct_supply_chain__stock_neshu, dupliqu\u00e9e volontairement : l'application ne lit pas les marts (cf. docs/conventions/apps.md). entity_type fait partie du grain : idcompany et idresources viennent de deux s\u00e9quences Oracle distinctes. plus/moins = cumuls d'entr\u00e9es/sorties depuis le dernier inventaire physique. date_system porte l'heure du batch ; filtrer sur snapshot_date.\n"""
    )
    as (
      

-- Statut COURANT des véhicules, non historisé : il est exposé sans filtrer, pour ne pas faire disparaître
-- rétroactivement de tout l'historique un véhicule désactivé depuis.
with vehicule as (
    select
        r.idresources,
        coalesce(lower(max(case when lf.code = 'ISACTIVE' then l.code end)) = 'yes', false) as is_active
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as r
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources_type` as rt
        on r.idresources_type = rt.idresources_type
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_resources` as lhr
        on r.idresources = lhr.idresources
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lhr.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
    where
        rt.code = 'VEHICLE'
        and r.code_status_record <> -1  -- ghost-deletes ERP
    group by r.idresources
)

select
    st.snapshot_date,
    st.id_entity,
    st.entity_type,
    st.product_code,
    st.resources_code as entity_code,
    st.entity_name,
    case when st.entity_type = 'resource' then v.is_active end as is_vehicle_active,
    st.product_name,
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
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu_gcs__stock_theorique` as st
left join vehicule as v
    on st.id_entity = v.idresources and st.entity_type = 'resource'
where
    (
        st.entity_type = 'company'
        and st.entity_name in (
            '01 - rungis depot produits',
            '02 - lyon depot produits',
            '03 - bordeaux depot produits',
            '04 - strasbourg depot produits',
            '05 - perimes depot',
            '10 - rebus depot',
            '13 - marseille depot produits'
        )
    )
    -- Seul filtre sur les ressources : le type VEHICLE, qui ne bouge pas ; il écarte la PERSON présente dans le stock.
    or (st.entity_type = 'resource' and v.idresources is not null)
    );
  