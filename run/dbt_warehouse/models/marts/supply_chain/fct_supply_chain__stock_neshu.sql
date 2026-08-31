
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu`
      
    partition by snapshot_date
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Stock th\u00e9orique journalier des produits Neshu, par v\u00e9hicule roadman et par d\u00e9p\u00f4t.\n[COMMENT CONSTRUITE] Issu de stg_oracle_neshu_gcs__stock_theorique (pipeline dlt oracle_neshu_stock, batch Oracle de 23:00 Paris), filtr\u00e9 sur les d\u00e9p\u00f4ts 01\u201305, 10, 13 (entity_type='company') et sur resources_type='VEHICLE' via dim_neshu__resource, qui exclut la PERSON pr\u00e9sente dans le stock. is_vehicle_active est aplati depuis la colonne is_active de la m\u00eame dim mais NE FILTRE PAS (FALSE si absent de la dim, NULL pour les d\u00e9p\u00f4ts). is_out_of_stock d\u00e9riv\u00e9 de stock_at_date=0. dpa fallback sur purchase_price si manquant.\n[GRAIN] 1 ligne par (snapshot_date, entity_type, id_entity, product_code).\n[NOTES] Filtrer sur snapshot_date : c'est une DATE, insensible \u00e0 l'heure du run comme au fuseau de lecture. date_system est conserv\u00e9e \u00e0 c\u00f4t\u00e9 pour l'audit \u2014 elle porte l'heure du batch, donc r\u00e9v\u00e8le une journ\u00e9e rejou\u00e9e \u2014 mais n'est pas une cl\u00e9 de filtre. entity_type fait partie du grain : idcompany et idresources viennent de deux s\u00e9quences Oracle distinctes et collisionnent (17 698 lignes concern\u00e9es au 2026-08-31). plus/moins = cumuls d'entr\u00e9es/sorties depuis le dernier inventaire physique, d'o\u00f9 l'invariant stock_at_date = stock_inventaire + plus - moins. P\u00c9RIM\u00c8TRE V\u00c9HICULES FIG\u00c9 depuis le 2026-08-31 : is_vehicle_active n'est plus filtrante, justement parce qu'elle porte l'\u00e9tat COURANT d'une dim non historis\u00e9e \u2014 la filtrer faisait dispara\u00eetre r\u00e9troactivement de tout l'historique un v\u00e9hicule d\u00e9sactiv\u00e9 depuis. Le total NON filtr\u00e9 inclut donc le r\u00e9sidu des v\u00e9hicules sortis du parc : au 2026-08-30, 24 v\u00e9hicules inactifs pour 796 lignes, dont le stock est FIG\u00c9 (aucun mouvement depuis le 30/06) et cumule +47 695 unit\u00e9s de r\u00e9sidu r\u00e9el contre -61 951 unit\u00e9s de th\u00e9orique n\u00e9gatif jamais sold\u00e9 \u2014 sujet de qualit\u00e9 de donn\u00e9e \u00e0 traiter avec Distrilog, pas de mod\u00e9lisation. Un rapport qui veut le parc roulant filtre sur is_vehicle_active, en acceptant que ce soit un \u00e9tat courant et non un \u00e9tat \u00e0 la date.\n"""
    )
    as (
      

with filtered_stock as (
    select
        st.snapshot_date,
        st.id_entity,
        st.entity_type,
        st.product_code,
        st.resources_code as entity_code,
        st.entity_name,
        -- Exposée, et NON filtrante : c'est un état COURANT, non historisé. La
        -- filtrer ici ferait disparaître rétroactivement de tout l'historique un
        -- véhicule désactivé depuis, et réapparaître un véhicule réactivé.
        case
            when st.entity_type = 'resource' then coalesce(r.is_active, false)
        end as is_vehicle_active,
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
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__resource` as r
        on
            st.id_entity = r.resources_id
            and st.entity_type = 'resource'
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
        or
        (
            st.entity_type = 'resource'
            -- Seul filtre conservé sur les ressources : le TYPE, qui ne bouge pas.
            -- Il exclut la PERSON présente dans le stock.
            and r.resources_type = 'VEHICLE'
        )
)

select
    snapshot_date,
    id_entity,
    entity_type,
    product_code,
    entity_code,
    entity_name,
    is_vehicle_active,
    product_name,
    stock_at_date,
    is_out_of_stock,
    date_inventaire,
    stock_inventaire,
    plus,
    moins,
    dpa,
    purchase_price,
    date_system,
    extracted_at
from filtered_stock
    );
  