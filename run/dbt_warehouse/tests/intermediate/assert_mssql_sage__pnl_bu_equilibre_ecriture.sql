
    
    select
      count(*) as failures,
      count(*) >60 as should_warn,
      count(*) >200 as should_error
    from (
      
    
  

-- Invariant comptable : pour chaque écriture ventilée, la somme des lignes
-- analytiques signées doit redonner le montant signé de l'écriture générale.
--
-- Garde-fou : `abs()` appliqué avant le sens inversait les réaffectations.
-- warn/error selon `warn_if`/`error_if`.

with ecritures_generales as (
    select
        ec_no as numero_ecriture_comptable,
        case when ec_sens = 0 then -ec_montant else ec_montant end as montant_general_signe
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturec`
),

ventilation as (
    select
        numero_ecriture_comptable,
        numero_plan_analytique,
        sum(montant_analytique_signe) as montant_analytique_signe
    from `evs-datastack-prod`.`prod_intermediate`.`int_mssql_sage__pnl_bu`
    where not is_missing_analytical
    group by numero_ecriture_comptable, numero_plan_analytique
)

select
    v.numero_ecriture_comptable,
    v.numero_plan_analytique,
    g.montant_general_signe,
    v.montant_analytique_signe,
    v.montant_analytique_signe - g.montant_general_signe as ecart
from ventilation as v
inner join ecritures_generales as g
    on v.numero_ecriture_comptable = g.numero_ecriture_comptable
where abs(v.montant_analytique_signe - g.montant_general_signe) >= 0.02
  
  
      
    ) dbt_internal_test