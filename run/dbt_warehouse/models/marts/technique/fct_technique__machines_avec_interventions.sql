
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_technique__machines_avec_interventions`
      
    
    cluster by n_serie_machine

    
    OPTIONS(
      description="""Nombre d'interventions techniques r\u00e9alis\u00e9es sur les machines install\u00e9es chez les clients\nsur les 12 derniers mois. Croise les donn\u00e9es clients Nespresso CO et les interventions\ntechniques Nespresso Tech.\n\n**Sources :** nesp_co (clients enrichis) \u00b7 nesp_tech (interventions d\u00e9dupliqu\u00e9es)\n"""
    )
    as (
      
-- Configuration dbt : table matérialisée + clustering sur n_serie_machine


-- Etats considérés comme terminés

with interventions_12m as (
-- CTE INTERVENTIONS_12M : filtre au plus tôt pour réduire le volume scanné
    select
        n_client,
        num_serie_machine,
        code_machine,
        n_planning,
        date_heure_fin
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
    where
        date_heure_fin >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), interval 365 day)
        and etat_intervention in (
            
                'terminée signée',
            
                'signature différée',
            
                'terminée non signée'
            
        )
),

clients as (
-- CTE CLIENTS : projection minimale (évite de trimballer des colonnes inutiles)
    select
        third,
        third_name,
        third_secteur,
        order_placer_name,
        order_placer_phone,
        third_address_1,
        third_address_2,
        third_city,
        third_post_code
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__clients_enrichis`
),

machines_clean as (
-- CTE MACHINES_CLEAN : normalisation avec LOWER côté dimension uniquement
    select
        LOWER(nom_machine) as nom_machine_clean,
        machine_clean
    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__machines_clean`
),

joined_data as (
-- CTE JOINED_DATA : jointures après réduction du volume (important en BQ)
    select
        i.n_client as mc,
        c.third_name as nom_client,
        c.third_secteur as secteur,
        c.order_placer_name as contact_client,
        c.order_placer_phone as telephone,
        CONCAT(COALESCE(c.third_address_1, ''), COALESCE(c.third_address_2, '')) as adresse,
        c.third_city as ville,
        c.third_post_code as code_postal,
        i.num_serie_machine as n_serie_machine,
        i.code_machine,
        m.machine_clean as machine,
        i.n_planning,
        i.date_heure_fin
    from interventions_12m as i
    inner join clients as c
        on i.n_client = c.third
    left join machines_clean as m
        on LOWER(i.code_machine) = m.nom_machine_clean
),

aggregated as (
-- CTE AGGREGATED : agrégation métier
    select
        mc,
        nom_client,
        secteur,
        contact_client,
        telephone,
        adresse,
        ville,
        code_postal,
        n_serie_machine,
        code_machine,
        machine,
        COUNT(*) as n_inter_12mois,
        MAX(date_heure_fin) as date_last_inter
    from joined_data
    group by
        mc,
        nom_client,
        secteur,
        contact_client,
        telephone,
        adresse,
        ville,
        code_postal,
        n_serie_machine,
        code_machine,
        machine
)

-- SELECT FINAL : déduplication déterministe
select
    mc,
    nom_client,
    secteur,
    contact_client,
    telephone,
    adresse,
    ville,
    code_postal,
    n_serie_machine,
    code_machine,
    machine,
    n_inter_12mois,
    date_last_inter
from aggregated
qualify
    ROW_NUMBER() over (
        partition by n_serie_machine
        order by date_last_inter desc, code_machine asc, mc asc
    ) = 1
    );
  