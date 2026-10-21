

-- Référentiel des techniciens TechCare pour Cockpit Supply, construit depuis le staging.
-- Technicien = utilisateur de type technician, ou manager déclaré technicien. L'entrepôt (camion) du
-- technicien porte le même identifiant que l'utilisateur dans Yuman.
select
    u.user_id,
    u.user_name,
    u.entrepot_rattachement,
    s.storehouses_name,
    u.is_active,
    u.user_type,
    -- Matricule Nomad (champ ID NOMAD de la fiche Yuman), clé des poses Nespresso : majuscules sans espaces,
    -- comme nomad_id de app_cockpit__technique_conso_nespresso_detail, pour une comparaison directe.
    upper(trim(u.nomad_id)) as nomad_id
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users` as u
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__storehouses` as s
    on u.user_id = s.storehouses_id
where
    u.user_type = 'technician'
    or (u.user_type = 'manager' and u.is_manager_as_technician = true)