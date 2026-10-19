{{ config(materialized='table') }}

-- Référentiel des techniciens TechCare pour Cockpit Supply, construit depuis le staging.
-- Technicien = utilisateur de type technician, ou manager déclaré technicien. L'entrepôt (camion) du
-- technicien porte le même identifiant que l'utilisateur dans Yuman.
select
    u.user_id,
    u.user_name,
    u.entrepot_rattachement,
    s.storehouses_name,
    u.is_active,
    u.user_type
from {{ ref('stg_yuman__users') }} as u
left join {{ ref('stg_yuman__storehouses') }} as s
    on u.user_id = s.storehouses_id
where
    u.user_type = 'technician'
    or (u.user_type = 'manager' and u.is_manager_as_technician = true)
