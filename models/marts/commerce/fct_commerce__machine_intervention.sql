{{
    config(
        materialized='table'
    )
}}

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
    from {{ ref('int_nesp_tech__interventions_dedup') }}
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
    from {{ ref('dim_commerce__client') }}
),

machines_clean as (
    select
        lower(nom_machine) as nom_machine_clean,
        machine_clean
    from {{ ref('ref_nesp_tech__machines_clean') }}
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
