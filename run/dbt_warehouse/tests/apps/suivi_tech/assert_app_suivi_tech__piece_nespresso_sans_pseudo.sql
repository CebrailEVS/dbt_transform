
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Aucun pseudo-article (ligne de saisie d'intervention) ne doit figurer dans le
-- référentiel des pièces ajoutables.
select d.product_reference
from `evs-datastack-prod`.`prod_app_suivi_tech`.`app_suivi_tech__technique_piece_nespresso` as d
inner join `evs-datastack-prod`.`prod_reference`.`ref_yuman__pseudo_article` as pa
    on lower(d.product_reference) = lower(pa.reference)
  
  
      
    ) dbt_internal_test