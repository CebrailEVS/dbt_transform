

-- ⚠️ Le temps retenu n'est PAS la somme des durées de tâches : c'est
-- l'AMPLITUDE de la journée chez le client, du début de la première
-- intervention à la fin de la dernière. Une pause entre deux machines est donc
-- comptée comme du temps passé. C'est la règle du rapport Distrilog, reproduite
-- telle quelle ; l'intention derrière reste à confirmer côté métier.
--
-- Deux comptages coexistent et ne mesurent pas la même chose :
--   nb_passages_machine = nombre de tâches (une par machine servie) ;
--   nb_passages_client  = nombre de jours distincts de visite.
--
-- Ne pas réutiliser int_oracle_neshu__appro_tasks : il sert le suivi
-- opérationnel et retient d'autres statuts.

with amplitude_par_jour as (

    select
        date_trunc(date(t.real_start_date), month) as mois,
        date(t.real_start_date) as jour,
        c.idcompany as company_id,
        c.code as company_code,
        -- Amplitude de la journée, en minutes.
        timestamp_diff(
            max(t.real_end_date), min(t.real_start_date), second
        ) / 60 as nb_min,
        -- distinct volontaire : la jointure sur task_has_resources peut
        -- démultiplier une tâche. Le raw porte 58 635 lignes de cette table
        -- supprimées à la source et jamais purgées (elle n'a pas weekly_purge),
        -- ce qui gonflait le comptage de 23 % sur août 2026. À la source une
        -- tâche d'appro n'a jamais qu'un approvisionneur : le distinct ne change
        -- donc rien au résultat attendu, il le protège.
        count(distinct t.idtask) as nb_passages_machine

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
        on t.iddevice = d.iddevice
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
        on t.idcompany_peer = c.idcompany and c.idcompany_type = 2  -- CUSTOMER
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_resources` as thr
        on t.idtask = thr.idtask
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as r
        on thr.idresources = r.idresources and r.idresources_type = 2  -- PERSON

    where
        t.idtask_type = 32  -- PASSAGE APPRO
        and t.idtask_status in (1, 4)  -- FAIT, VALIDE
        and t.code_status_record = '1'
        and t.real_start_date is not null
        and t.real_end_date is not null

    group by mois, jour, company_id, company_code
)

select
    mois,
    company_id,
    company_code,
    round(sum(nb_min), 0) as nb_min_rm,
    sum(nb_passages_machine) as nb_passages_machine,
    count(distinct jour) as nb_passages_client
from amplitude_par_jour
group by mois, company_id, company_code