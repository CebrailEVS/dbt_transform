
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_commerce__machines_avec_interventions`
      
    
    

    
    OPTIONS(
      description="""Table d\u00e9taill\u00e9e des interventions techniques cl\u00f4tur\u00e9es sur les 6 derniers mois, enrichie des informations client et du libell\u00e9 machine normalis\u00e9. Granularit\u00e9 : 1 ligne par intervention.\n"""
    )
    as (
      

with interventions as (
    select
        n_client,
        num_serie_machine,
        code_machine,
        nom_machine,
        n_planning,
        date_heure_fin,
        consignes,
        observations,
        etat_intervention
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
    where
        date(date_heure_fin) >= date_sub(current_date(), interval 6 month)
        and etat_intervention in (
            'terminée signée',
            'signature différée',
            'terminée non signée'
        )
),

clients as (
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
    select
        lower(nom_machine) as nom_machine_clean,
        machine_clean
    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__machines_clean`
)

select
    i.n_client as mc,
    c.third_name as nom_client,
    concat(cast(i.n_client as string), ' - ', c.third_name) as mc_nom_client,
    c.third_secteur as secteur,
    c.order_placer_name as contact_client,
    c.order_placer_phone as telephone,
    concat(
        coalesce(c.third_address_1, ''),
        ' ',
        coalesce(c.third_address_2, '')
    ) as adresse,
    c.third_city as ville,
    c.third_post_code as code_postal,
    i.num_serie_machine as n_serie_machine,
    i.code_machine,
    i.nom_machine,
    m.machine_clean as machine,
    i.n_planning as intervention,
    i.date_heure_fin as date_intervention,
    i.consignes,
    i.observations,
    i.etat_intervention
from interventions as i
inner join clients as c
    on i.n_client = c.third
left join machines_clean as m
    on lower(i.nom_machine) = m.nom_machine_clean
    );
  