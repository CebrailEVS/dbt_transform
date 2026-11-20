
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- STOCK VAN INCONNU : toute ligne signalée a un van connu et une pièce contrôlée dont le stock
-- est null (van absent de l'export retenu), et aucune ligne ne porte à la fois ce libellé et
-- POSE SANS STOCK. Retourne une ligne par écart.
with stock_van_inconnu as (
    select
        d.n_planning,
        d.van_technicien is not null as van_connu,
        exists(
            select 1
            from unnest(d.pieces) as p
            inner join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__piece_controle_stock` as pc
                on p.code_article = pc.code_article
            where p.stock_van_veille is null
        ) as a_une_piece_sans_stock_connu,
        d.alerte_compile like '%POSE SANS STOCK%' as a_pose_sans_stock
    from `evs-datastack-prod`.`prod_app_suivi_tech`.`app_suivi_tech__technique_alerte_conso_aguila` as d
    where d.alerte_compile like '%STOCK VAN INCONNU%'
)

select
    n_planning,
    van_connu,
    a_une_piece_sans_stock_connu,
    a_pose_sans_stock
from stock_van_inconnu
where not van_connu or not a_une_piece_sans_stock_connu or a_pose_sans_stock
  
  
      
    ) dbt_internal_test