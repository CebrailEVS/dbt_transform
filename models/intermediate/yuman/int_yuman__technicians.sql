{{ config(
    materialized='table',
    schema='intermediate',
    alias='int_yuman__technicians'
) }}

with
source as (
    select * from {{ ref('stg_yuman__users') }}
),
technicians as (
    -- Techniciens purs + managers intervenant également en tant que technicien
    select *
    from source
    where
        user_type = 'technician'
        or (user_type = 'manager' and is_manager_as_technician = true)
),
final as (
    select
        -- ----------------------------------------------------------------
        -- Identifiants
        -- ----------------------------------------------------------------
        t.user_id,              -- PK : identifiant unique de l'utilisateur dans Yuman
        t.nomad_id,             -- Identifiant dans le système Nomad (nullable)
        t.manager_id,           -- FK vers user_id du manager direct
        -- ----------------------------------------------------------------
        -- Attributs du technicien
        -- ----------------------------------------------------------------
        t.user_name,            -- Nom complet affiché
        t.user_email,           -- Adresse e-mail professionnelle
        t.user_phone,           -- Numéro de téléphone
        t.user_type,            -- 'technician' | 'manager' (manager-technicien uniquement)
        t.user_secteur,         -- Secteur géographique ou opérationnel d'affectation
        t.is_manager_as_technician, -- true si le manager intervient aussi en tant que technicien
        t.is_active,            -- false si l'utilisateur a été désactivé dans Yuman (champ INACTIF)
        -- ----------------------------------------------------------------
        -- Libellé du manager (self-join sur stg_yuman__users)
        -- ----------------------------------------------------------------
        m.user_name             as manager_name, -- Nom complet du manager direct (null si aucun manager)
        -- ----------------------------------------------------------------
        -- Timestamps techniques
        -- ----------------------------------------------------------------
        t.created_at,           -- Date de création de l'utilisateur dans Yuman
        t.updated_at,           -- Dernière mise à jour dans Yuman
        t.extracted_at,         -- Dernière extraction depuis l'API Yuman
        t.deleted_at            -- Date de suppression logique (null si non supprimé)
    from technicians t
    left join source m
        on t.manager_id = m.user_id
)
select
    user_id,
    nomad_id,
    manager_id,
    user_name,
    user_email,
    user_phone,
    user_type,
    user_secteur,
    is_manager_as_technician,
    is_active,
    manager_name,
    created_at,
    updated_at,
    extracted_at,
    deleted_at
from final