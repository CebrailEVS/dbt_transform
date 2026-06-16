
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__comptage_tasks`
      
    partition by timestamp_trunc(task_start_date, day)
    cluster by company_id, device_id

    
    OPTIONS(
      description="""Comptages de caisse (REGL COMPTAGE, idtask_type=30) : CA encaiss\u00e9 en esp\u00e8ces (pi\u00e8ces + billets) par machine et par date de comptage. Sert \u00e0 compl\u00e9ter le CA des machines \u00e0 monnayeur (ventes en pi\u00e8ces invisibles c\u00f4t\u00e9 t\u00e9l\u00e9m\u00e9trie Nayax). 1 ligne par comptage (task_id). Montants = argent physique relev\u00e9 (TTC par nature, tax_rate=0). Cadence ~1 comptage / 1-2 semaines \u2192 analyser \u00e0 un grain >= mensuel.\n"""
    )
    as (
      

with comptage_base as (

    select
        -- Identifiants
        t.idtask as task_id,
        t.iddevice as device_id,
        t.idcompany_peer as company_id,
        t.idlocation as location_id,

        -- Codes / infos métier
        d.code as device_code,
        c.code as company_code,
        c.name as company_name,
        l.access_info as task_location_info,

        -- Date du comptage (récupération des pièces)
        t.real_start_date as task_start_date,

        -- Montants encaissés (argent physique relevé au comptage).
        -- sale_amount_net = sale_amount_net_tax et tax_rate = 0 → montant brut encaissé,
        -- donc TTC par nature (ce que le client a payé). À combiner au CA Nayax TTC.
        sum(case when thp.code = 'PIECES' then thp.sale_amount_net else 0 end) as ca_pieces_eur,
        sum(case when thp.code = 'BILLET' then thp.sale_amount_net else 0 end) as ca_billets_eur,
        sum(
            case when thp.code in ('PIECES', 'BILLET') then thp.sale_amount_net else 0 end
        ) as ca_cash_eur,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_product` as thp
        on t.idtask = thp.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device` as d on t.iddevice = d.iddevice
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as c on t.idcompany_peer = c.idcompany
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__location` as l on t.idlocation = l.idlocation

    where
        1 = 1
        and t.idtask_type = 30  -- REGL COMPTAGE
        and t.code_status_record = '1'
        and t.real_start_date is not null
        and thp.code in ('PIECES', 'BILLET')

    group by
        t.idtask, t.iddevice, t.idcompany_peer, t.idlocation,
        d.code, c.code, c.name, l.access_info,
        t.real_start_date,
        t.updated_at, t.created_at, t.extracted_at
)

select * from comptage_base


    );
  