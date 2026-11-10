
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
-- Moins de 80 % des livraisons LOG → TAMPON du dernier mois complet portent un n° de facture e-shop :
-- la saisie « ES nnnn » a changé ou s'est arrêtée, et l'app perd la clé du rapprochement commande par commande.
with livraisons as (
    select
        task_id,
        logical_or(numero_facture_eshop is not null) as has_numero
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_livraison_interne`
    where
        source_code = 'LOG'
        and destination_code = 'TAMPON'
        and date_trunc(date(task_start_date), month)
        = date_sub(date_trunc(current_date('Europe/Paris'), month), interval 1 month)
    group by task_id
)

select
    count(*) as nb_livraisons,
    countif(has_numero) as nb_avec_numero
from livraisons
having count(*) > 0 and countif(has_numero) < 0.8 * count(*)
  
  
      
    ) dbt_internal_test