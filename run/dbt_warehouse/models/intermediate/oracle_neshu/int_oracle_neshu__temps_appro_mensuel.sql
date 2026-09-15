
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__temps_appro_mensuel`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Temps de pr\u00e9sence des approvisionneurs chez le client et comptages de passages, par mois. Base de la r\u00e9partition du co\u00fbt de main-d'oeuvre du P&L.\n[COMMENT CONSTRUITE] T\u00e2ches PASSAGE APPRO (type 32), statuts 1/4, servies par une ressource de type PERSON. Amplitude calcul\u00e9e par jour et par client, puis somm\u00e9e sur le mois.\n[GRAIN] 1 ligne par (mois, company_id).\n[NOTES] Le temps retenu est l'AMPLITUDE de la journ\u00e9e chez le client \u2014 de la premi\u00e8re arriv\u00e9e au dernier d\u00e9part \u2014 et non la somme des dur\u00e9es de t\u00e2ches : une pause entre deux machines est compt\u00e9e comme du temps pass\u00e9. R\u00e8gle de Distrilog, reproduite telle quelle ; l'intention reste \u00e0 confirmer c\u00f4t\u00e9 m\u00e9tier. nb_passages_machine utilise un count DISTINCT volontaire : le raw porte 58 635 lignes de task_has_resources supprim\u00e9es \u00e0 la source et jamais purg\u00e9es (cette table n'a pas weekly_purge), ce qui gonflait le comptage de 23 % sur ao\u00fbt 2026. \u00c0 la source une t\u00e2che d'appro n'a qu'un approvisionneur, le distinct ne change donc rien au r\u00e9sultat attendu \u2014 il le prot\u00e8ge. Ne pas r\u00e9utiliser int_oracle_neshu__appro_tasks, qui sert l'op\u00e9rationnel et retient d'autres statuts.\n"""
    )
    as (
      

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
    );
  