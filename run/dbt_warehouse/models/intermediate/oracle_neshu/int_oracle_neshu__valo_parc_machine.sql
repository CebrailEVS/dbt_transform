
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__valo_parc_machine`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with machine_infos as (

  select
    d.iddevice,
    d.idcompany_customer,
    p.name as product_name
  from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` d
  left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` p
    on d.idmodel = p.idproduct
  left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__location` l
    on d.idlocation = l.idlocation
  where d.iddevice_type = 1

),

companies as (

  select
    idcompany,
    code as company_code,
    name as company_name
  from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company`

),

filtered_machines as (

  select
    lower(trim(mi.product_name)) as machine_modele
  from machine_infos mi
  left join companies c
    on mi.idcompany_customer = c.idcompany
  where c.company_name not in (
    '06 - ATELIER RUNGIS DEPOT',
    '07 - ATELIER LYON DEPOT',
    '08 - ATELIER BORDEAUX DEPOT',
    '10 - REBUS DEPOT',
    '11 - REMISE EN ETAT - RUNGIS',
    '12 - REMISE EN ETAT - LYON'
  )

),

counts as (

  select
    machine_modele,
    count(*) as count_machine
  from filtered_machines
  group by machine_modele

),

ref_valo as (

  select
    lower(trim(machine_modele)) as machine_modele,
    montant_inv as montant_unitaire
  from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__valo_parc_machine`

),

-- Changement principal : utiliser FULL OUTER JOIN
joined as (

  select
    coalesce(c.machine_modele, v.machine_modele) as machine_modele,
    v.montant_unitaire,
    coalesce(c.count_machine, 0) as count_machine,
    case
      when v.montant_unitaire is null then null
      else v.montant_unitaire * coalesce(c.count_machine, 0)
    end as montant_inv_total
  from counts c
  full outer join ref_valo v  -- Changement ici
    on c.machine_modele = v.machine_modele

)

select
  machine_modele,
  montant_unitaire,
  count_machine,
  montant_inv_total
from joined
    );
  