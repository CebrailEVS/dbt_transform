

-- Référentiel des sociétés Neshu (clients, fournisseurs, dépôts), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    company_id,
    company_code,
    company_name,
    is_depot
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__company`