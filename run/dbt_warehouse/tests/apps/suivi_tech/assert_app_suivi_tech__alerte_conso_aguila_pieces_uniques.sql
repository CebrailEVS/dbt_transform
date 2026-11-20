
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Une pièce n'apparaît qu'une fois par intervention : l'application retraite une pièce
-- par (n_planning, code_article), un doublon compterait deux fois la même pose.
select
    d.n_planning,
    p.code_article,
    count(*) as nb
from `evs-datastack-prod`.`prod_app_suivi_tech`.`app_suivi_tech__technique_alerte_conso_aguila` as d
cross join unnest(d.pieces) as p
group by d.n_planning, p.code_article
having count(*) > 1
  
  
      
    ) dbt_internal_test