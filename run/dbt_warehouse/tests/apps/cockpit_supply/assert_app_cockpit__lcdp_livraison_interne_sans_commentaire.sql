
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Le commentaire Distrilog d'une livraison interne est un texte libre (noms de clients) : seul le n° de
-- facture e-shop en est extrait. Toute colonne de commentaire dans la table de l'app fait échouer le test.
select column_name
from `evs-datastack-prod`.`prod_app_cockpit_supply`.INFORMATION_SCHEMA.COLUMNS  -- noqa: CP02
where
    table_name = 'app_cockpit__lcdp_livraison_interne'
    and regexp_contains(lower(column_name), r'comment')
  
  
      
    ) dbt_internal_test