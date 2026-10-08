

-- Valorisation du parc de machines Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    device_group,
    device_name,
    nombre_machines,
    valorisation_totale_machine
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__valorisation_parc_machines`