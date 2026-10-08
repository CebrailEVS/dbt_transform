

-- Référentiel des sociétés Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    company_id,
    company_code,
    company_name
from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__company`