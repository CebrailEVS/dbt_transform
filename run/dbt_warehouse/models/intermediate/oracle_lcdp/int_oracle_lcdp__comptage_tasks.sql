-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__comptage_tasks` as DBT_INTERNAL_DEST
        using (

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
),

-- Ventilation HT / TVA par tâche (somme des taux) depuis TASK_HAS_AMOUNT
amount_per_task as (
    select
        idtask,
        sum(amount_without_tax) as ca_cash_ht_eur,
        sum(tax_amount) as ca_cash_tva_eur
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_amount`
    group by idtask
),

comptage_enrichi as (
    select
        cb.task_id,
        cb.device_id,
        cb.company_id,
        cb.location_id,
        cb.device_code,
        cb.company_code,
        cb.company_name,
        cb.task_location_info,
        cb.task_start_date,
        cb.ca_pieces_eur,
        cb.ca_billets_eur,
        cb.ca_cash_eur,
        a.ca_cash_ht_eur,
        a.ca_cash_tva_eur,
        cb.updated_at,
        cb.created_at,
        cb.extracted_at
    from comptage_base as cb
    left join amount_per_task as a on cb.task_id = a.idtask
)

select * from comptage_enrichi


    where comptage_enrichi.updated_at >= (
        select max(t.updated_at) - interval 1 day
        from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__comptage_tasks` as t
    )

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.task_id = DBT_INTERNAL_DEST.task_id))

    
    when matched then update set
        `task_id` = DBT_INTERNAL_SOURCE.`task_id`,`device_id` = DBT_INTERNAL_SOURCE.`device_id`,`company_id` = DBT_INTERNAL_SOURCE.`company_id`,`location_id` = DBT_INTERNAL_SOURCE.`location_id`,`device_code` = DBT_INTERNAL_SOURCE.`device_code`,`company_code` = DBT_INTERNAL_SOURCE.`company_code`,`company_name` = DBT_INTERNAL_SOURCE.`company_name`,`task_location_info` = DBT_INTERNAL_SOURCE.`task_location_info`,`task_start_date` = DBT_INTERNAL_SOURCE.`task_start_date`,`ca_pieces_eur` = DBT_INTERNAL_SOURCE.`ca_pieces_eur`,`ca_billets_eur` = DBT_INTERNAL_SOURCE.`ca_billets_eur`,`ca_cash_eur` = DBT_INTERNAL_SOURCE.`ca_cash_eur`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`task_id`, `device_id`, `company_id`, `location_id`, `device_code`, `company_code`, `company_name`, `task_location_info`, `task_start_date`, `ca_pieces_eur`, `ca_billets_eur`, `ca_cash_eur`, `updated_at`, `created_at`, `extracted_at`)
    values
        (`task_id`, `device_id`, `company_id`, `location_id`, `device_code`, `company_code`, `company_name`, `task_location_info`, `task_start_date`, `ca_pieces_eur`, `ca_billets_eur`, `ca_cash_eur`, `updated_at`, `created_at`, `extracted_at`)


    