
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__opportunites`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Opportunit\u00e9s commerciales Nespresso (pipeline de vente, source C4C/Nessoft) : une ligne par opportunit\u00e9, avec son statut, le compte, le commercial, la campagne et la valeur attendue.\n[COMMENT CONSTRUITE] stg_nesp_co__opportunite : renommage m\u00e9tier (pr\u00e9fixe opp_), traduction FR des statuts et r\u00f4les, normalisation des identifiants (C4C '#' \u2192 NULL), probabilit\u00e9 ramen\u00e9e en [0,1].\n[GRAIN] 1 ligne par opp_id (PK). ~27,7k lignes.\n[NOTES] Source commerciale Nespresso, encore en construction (WIP). Les montants et probabilit\u00e9s sont du pr\u00e9visionnel commercial, pas du r\u00e9alis\u00e9.\n"""
    )
    as (
      

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

    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__opportunite`

),

enrich as (

    select

        -- =============================
        -- Clé opportunité
        -- =============================
        cast(opportunity_id as string) as opp_id,
        opportunity_name as opp_nom,

        -- =============================
        -- Dates
        -- =============================
        created_on as opp_date_creation,
        close_date as opp_date_cloture,

        -- =============================
        -- Compte
        -- =============================
        account_name as opp_compte,
        nessoft_id_account as opp_id_compte,
        nullif(cast(c4c_id_account as string), '#') as opp_id_client_c4c,

        -- =============================
        -- Commercial
        -- =============================
        c4c_id_commercial as opp_id_commercial,

        -- =============================
        -- Campagne
        -- =============================
        campaign_name as opp_campagne,
        safe_cast(c4c_id_campaign as int64) as opp_campagne_id,

        -- =============================
        -- Métadonnées
        -- =============================
        zeq_zenius_equivalent as opp_ez,
        source_name as opp_source,

        -- =============================
        -- Statut traduit
        -- =============================
        case lifecycle_status
            when 'Won' then 'Gagné'
            when 'Lost' then 'Perdu'
            when 'In Process' then 'En cours'
            when 'Open' then 'En cours'
            else lifecycle_status
        end as opp_statut,

        -- =============================
        -- Rôle traduit
        -- =============================
        case role_account
            when 'Customer' then 'Client'
            when 'Prospect' then 'Client potentiel'
            else role_account
        end as opp_role,

        -- =============================
        -- KPI
        -- =============================
        first_coffee_order as opp_first_order_cafe,
        expected_value as opp_ns_attendu,
        chance_of_success / 100 as opp_probabilite,
        machines_opportunity as opp_nb_opport

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
    );
  