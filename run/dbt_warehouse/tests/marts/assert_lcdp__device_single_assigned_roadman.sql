
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

-- Alerte (warn) : machines portant PLUSIEURS affectations roadman dans l'ERP
-- (contact_has_device). dim_lcdp__device n'en expose qu'une, départagée sur le
-- code le plus petit, afin de garantir 1 ligne par machine.
--
-- Ce test ne dit pas qu'il y a un bug : il rend visible le nombre de cas où le
-- départage s'applique réellement. Au 2026-07-29 il y en a 2 (M7077, M5114, des
-- machines à café hors périmètre DA FROID). Si ce nombre grimpe, la règle
-- « code le plus petit » ne suffit plus et il faudra arbitrer avec l'exploitation
-- (affectation principale à qualifier dans l'ERP, ou grain machine × roadman).
select
    chd.iddevice as device_id,
    count(distinct r.resources_id) as nb_roadmen_affectes,
    string_agg(distinct r.resources_code order by r.resources_code) as codes_roadmen
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__contact_has_device` as chd
inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__contact` as ct
    on chd.idcontact = ct.idcontact
inner join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__resource` as r
    on ct.code = r.resources_code and r.resources_type = 'PERSON'
group by chd.iddevice
having count(distinct r.resources_id) > 1
  
  
      
    ) dbt_internal_test