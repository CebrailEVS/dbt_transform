
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_yuman__workorder_delais_neshu`
      
    partition by timestamp_trunc(date_done, day)
    

    
    OPTIONS(
      description="""Table de fait marts de tarification automatique des interventions (workorders). Ce mod\u00e8le enrichit les donn\u00e9es unifi\u00e9es des interventions et des demandes issues de  `int_yuman__demands_workorders_enriched` avec les r\u00e9f\u00e9rentiels de types d'intervention, machines, tarifications et zones g\u00e9ographiques.  Il calcule les prix automatiques des interventions selon les r\u00e8gles de r\u00e9currence,  de partenaire et de localisation (m\u00e9tropole).\n"""
    )
    as (
      

with adjusted_dates as (
    select
        sfp.workorder_id as sfp_numero,
        coalesce(timestamp(sfp.workorder_date_creation), sfp.demand_created_at) as date_creation_initial,
        sfp.date_done as date_fin,
        case
            when extract(time from coalesce(timestamp(sfp.workorder_date_creation), sfp.demand_created_at)) > '16:00:00'
                then timestamp(date(coalesce(timestamp(sfp.workorder_date_creation), sfp.demand_created_at)) + 1)
            else coalesce(timestamp(sfp.workorder_date_creation), sfp.demand_created_at)
        end as date_creation_ref
    from `evs-datastack-prod`.`prod_marts`.`fct_yuman__workorder_pricing` as sfp
),

dates_range as (
    select
        ad.sfp_numero,
        ad.date_creation_initial,
        ad.date_fin,
        ad.date_creation_ref,
        date_jour
    from adjusted_dates as ad,
        unnest(
            generate_date_array(
                date(ad.date_creation_ref),
                date(ad.date_fin),
                interval 1 day
            )
        ) as date_jour
),

jours_feries as (
    select *
    from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_metropole`
),

famille_machine as (
    select
        machine_brut,
        machine,
        famille_neshu
    from `evs-datastack-prod`.`prod_reference`.`ref_yuman__machine_clean`
),

jours_ouvrables as (
    select
        dr.sfp_numero,
        dr.date_creation_initial,
        dr.date_fin,
        dr.date_creation_ref,
        dr.date_jour,
        jf.date_ferie
    from dates_range as dr
    left join jours_feries as jf
        on dr.date_jour = jf.date_ferie
    where
        extract(dayofweek from dr.date_jour) not in (1, 7)
        and jf.date_ferie is null
),

delai_calcul as (
    select
        jo.sfp_numero,
        jo.date_creation_initial,
        jo.date_fin,
        jo.date_creation_ref,
        count(jo.date_jour) - 1 as delai_jours_ouvres
    from jours_ouvrables as jo
    group by
        jo.sfp_numero,
        jo.date_creation_initial,
        jo.date_fin,
        jo.date_creation_ref
),

final_table as (
    select
        sfp.*,
        dc.date_creation_ref,
        dc.delai_jours_ouvres,
        case
            when dc.delai_jours_ouvres = 0
                then 'J+0,5'
            when
                dc.delai_jours_ouvres = 1
                and extract(time from dc.date_creation_ref) > '12:00:00'
                and extract(time from dc.date_fin) < '12:00:00'
                then 'J+0,5'
            when dc.delai_jours_ouvres = 1
                then 'J+1'
            when dc.delai_jours_ouvres = 2
                then 'J+2'
            when dc.delai_jours_ouvres > 2
                then 'J++'
            else 'ERREUR'
        end as type_delai,
        fm.famille_neshu
    from `evs-datastack-prod`.`prod_marts`.`fct_yuman__workorder_pricing` as sfp
    left join delai_calcul as dc
        on sfp.workorder_id = dc.sfp_numero
    left join famille_machine as fm
        on sfp.machine_raw = fm.machine_brut
)

select *
from final_table
    );
  