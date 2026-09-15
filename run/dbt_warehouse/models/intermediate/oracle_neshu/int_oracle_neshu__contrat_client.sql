
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__contrat_client`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Date du premier contrat de chaque client, qui date l'entr\u00e9e en relation et fonde la typologie GET / OTHER du P&L.\n[COMMENT CONSTRUITE] Minimum de original_start_date par idcompany_peer sur stg_oracle_neshu__contract.\n[GRAIN] 1 ligne par company_id.\n[NOTES] Un client peut porter plusieurs contrats \u2014 314 contrats pour 240 clients au 2026-09-15 \u2014 d'o\u00f9 le minimum. La typologie elle-m\u00eame n'est PAS calcul\u00e9e ici : elle d\u00e9pend de l'ann\u00e9e de r\u00e9f\u00e9rence, donc du mois analys\u00e9, et vit au grain (mois, client) dans le mart.\n"""
    )
    as (
      

-- Un client peut porter plusieurs contrats (314 contrats pour 240 clients au
-- 2026-09-15). Le rapport Distrilog retient le plus ancien : c'est lui qui date
-- l'entrée en relation, donc la typologie.
--
-- La typologie elle-même (GET = acquis dans l'année) n'est PAS calculée ici : elle
-- dépend de l'année de référence, donc du mois analysé. Elle vit au grain
-- (mois, client), dans le P&L.

with contrats as (

    select
        idcompany_peer as company_id,
        original_start_date,
        updated_at,
        extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__contract`
    where
        idcompany_peer is not null
        and original_start_date is not null
)

select
    company_id,
    min(original_start_date) as first_contract_date,
    count(*) as nb_contrats,
    max(updated_at) as updated_at,
    max(extracted_at) as extracted_at
from contrats
group by company_id
    );
  