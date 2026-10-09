
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select contrat_immatriculation_edi
from `evs-datastack-prod`.`prod_intermediate`.`int_gac__vehicule_code_analytique`
where contrat_immatriculation_edi is null



  
  
      
    ) dbt_internal_test