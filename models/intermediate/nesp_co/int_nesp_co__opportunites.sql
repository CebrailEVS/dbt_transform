{{ config(
    materialized = 'table',
    tags = ['intermediate', 'nesp_co', 'opportunites'],
    description='Liste des Opportunités des commerciaux'
) }}

with base as (

    select
        -- Champs bruts
        opportunity_id,
        opportunity_name,
        zeq_zenius_equivalent,
        created_on,
        source_name,
        account_name,
        nessoft_id_account,
        lifecycle_status,
        close_date,
        c4c_id_account,
        role_account,
        c4c_id_commercial,
        campaign_name,
        c4c_id_campaign,
        first_coffee_order,
        expected_value,
        chance_of_success,
        machines_opportunity

    from {{ref('stg_nesp_co__opportunite')}}

),

enrich as (

    select

        -- =============================
        -- Clé opportunité
        -- =============================
        cast(opportunity_id as string)                    as opp_id,
        opportunity_name                                  as opp_nom,

        -- =============================
        -- Dates
        -- =============================
        created_on                                        as opp_date_creation,
        close_date                                        as opp_date_cloture,

        -- =============================
        -- Compte
        -- =============================
        account_name                                      as opp_compte,
        nessoft_id_account                                as opp_id_compte,
        nullif(cast(c4c_id_account as string), '#')       as opp_id_client_c4c,

        -- =============================
        -- Commercial
        -- =============================
        c4c_id_commercial                                 as opp_id_commercial,

        -- =============================
        -- Campagne
        -- =============================
        campaign_name                                     as opp_campagne,
        safe_cast(c4c_id_campaign as int64)               as opp_campagne_id,

        -- =============================
        -- Métadonnées
        -- =============================
        zeq_zenius_equivalent                              as opp_ez,
        source_name                                        as opp_source,

        -- =============================
        -- Statut traduit
        -- =============================
        case lifecycle_status
            when 'Won' then 'Gagné'
            when 'Lost' then 'Perdu'
            when 'In Process' then 'En cours'
            when 'Open' then 'En cours'
            else lifecycle_status
        end                                                as opp_statut,

        -- =============================
        -- Rôle traduit
        -- =============================
        case role_account
            when 'Customer' then 'Client'
            when 'Prospect' then 'Client potentiel'
            else role_account
        end                                                as opp_role,

        -- =============================
        -- KPI
        -- =============================
        first_coffee_order                                 as opp_first_order_cafe,
        expected_value                                     as opp_ns_attendu,
        chance_of_success / 100                            as opp_probabilite,
        machines_opportunity                               as opp_nb_opport

    from base

)

select

    opp_id,
    opp_nom,
    opp_ez,
    opp_date_creation,
    opp_source,
    opp_compte,
    opp_id_compte,
    opp_id_client_c4c,
    opp_statut,
    opp_date_cloture,
    opp_role,
    opp_id_commercial,
    opp_campagne,
    opp_campagne_id,
    opp_first_order_cafe,
    opp_ns_attendu,
    opp_probabilite,
    opp_nb_opport

from enrich