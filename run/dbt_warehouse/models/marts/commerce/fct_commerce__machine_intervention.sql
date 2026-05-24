
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_commerce__machine_intervention`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nInterventions techniques EVS sur le parc machines Nespresso, cl\u00f4tur\u00e9es\nsur les 6 derniers mois glissants. Vue consolid\u00e9e Nespresso pour le suivi\ncommercial / activit\u00e9.\n\n[COMMENT CONSTRUITE]\nIssu de int_nesp_tech__interventions_dedup (interventions Nespresso re\u00e7ues\npar fichier Excel) filtr\u00e9 sur \u00e9tats cl\u00f4tur\u00e9s ('termin\u00e9e sign\u00e9e', 'signature\ndiff\u00e9r\u00e9e', 'termin\u00e9e non sign\u00e9e') et date_heure_fin >= today - 6 mois.\nJoint \u00e0 int_nesp_co__clients_enrichis pour les coordonn\u00e9es client (n_client\n= third). Joint \u00e0 ref_nesp_tech__machines_clean pour normaliser le libell\u00e9\nmachine.\n\n[GRAIN]\n1 ligne par intervention (n_planning).\n\n[NOTES]\nSources nesp_tech (fichier Excel hebdo Mon 07:30) et nesp_co (fichier Excel\ndaily 08:00) \u2014 pas de base de donn\u00e9es, donc pas de vraies dims, les\nattributs client/machine sont aplatis depuis les intermediates. INNER JOIN\nsur le client : une intervention sans client matching est exclue. Rafra\u00eechi\n2\u00d7 par jour weekdays (08:30 + 10:00) via transform-commerce-daily.\n"""
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
  