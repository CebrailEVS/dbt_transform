
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_neshu__workorder_delai`
      
    partition by timestamp_trunc(date_done, day)
    cluster by client_id, site_id, material_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] D\u00e9lai de traitement des interventions Yuman (workorders) et tarification automatique associ\u00e9e.\n[COMMENT CONSTRUITE] Fait mince lisant l'interm\u00e9diaire int_yuman__interventions (source de v\u00e9rit\u00e9 unique consolidant demandes + bons de travail Yuman, tarification automatique et d\u00e9lai), sans d\u00e9pendance fait\u2192fait. Le d\u00e9lai en jours ouvr\u00e9s y est calcul\u00e9 via generate_date_array entre date_creation_ref (cr\u00e9a workorder, ou demande si workorder NULL ; d\u00e9cal\u00e9e au lendemain si > 16h) et date_done. Type de d\u00e9lai selon r\u00e8gles NESHU valid\u00e9es par le Responsable Technique.\n[GRAIN] 1 ligne par workorder_id (et par demand_id \u2014 bijection test\u00e9e par unique).\n[NOTES] billing_validation_status \u2208 {VALIDATED, MISSING_TARIF, NOT_BILLABLE}. pricing_type \u2208 {Tarif normal, Remise niv1, Remise niv2}.\n"""
    )
    as (
      

-- Fait mince : délai de traitement et tarification des interventions Yuman.
-- Toute la logique métier (normalisation, tarification, calcul du délai en jours
-- ouvrés) vit désormais dans int_yuman__interventions. Ce modèle ne fait qu'exposer
-- le contrat de colonnes historique attendu par le rapport Power BI
-- (exposure neshu / workorder_delai). Aucune dépendance fait→fait.

select
    demand_id,
    workorder_id,
    material_id,
    site_id,
    client_id,
    technician_id,
    manager_id,
    demand_description,
    demand_status,
    demand_created_at,
    demand_updated_at,
    demand_category_name,
    workorder_number,
    workorder_category,
    workorder_status,
    workorder_technician_name,
    workorder_date_creation,
    workorder_report,
    workorder_motif_non_intervention,
    workorder_detail_non_intervention,
    workorder_raison_mise_en_pause,
    workorder_explication_mise_en_pause,
    date_planned,
    date_started,
    date_done,
    partner_name,
    client_code,
    client_name,
    client_category,
    client_is_active,
    site_code,
    site_name,
    site_address,
    site_postal_code,
    material_serial_number,
    workorder_type_raw,
    machine_raw,
    workorder_type_clean,
    machine_clean,
    metropolitan,
    metropole_city,
    technician_equipe,
    recurrence_count,
    pricing_type,
    pricing_key_used,
    to_invoice,
    amount,
    prod_number,
    billing_validation_status,
    date_creation_ref,
    delai_jours_ouvres,
    type_delai,
    famille_neshu
from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__interventions`
    );
  