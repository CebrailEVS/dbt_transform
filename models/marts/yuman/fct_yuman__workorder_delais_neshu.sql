{{ config(
    materialized = "table",
    schema='marts',
    alias = "fct_yuman__workorder_delais_neshu",
    partition_by={"field": "date_done", "data_type": "timestamp"},
    cluster_by=['workorder_type_clean','partner_name','workorder_status']
) }}

-- ============================================================================
-- MODEL: fct_yuman__workorder_delais_neshu
-- PURPOSE: Determiner les delais des inter (notamment curative) pas partenaire
-- AUTHOR: Etienne BOULINIER
-- ============================================================================

WITH adjusted_dates AS (
    -- Calculer la date de référence (ajouter 1 jour si l'heure dépasse 16h)
    select
    	sfp.workorder_id as sfp_numero,
        coalesce(TIMESTAMP(sfp.workorder_date_creation),sfp.demand_created_at) as date_creation_initial,
        sfp.date_done as date_fin,
        CASE
          WHEN EXTRACT(TIME FROM COALESCE(TIMESTAMP(sfp.workorder_date_creation), sfp.demand_created_at)) > '16:00:00'
            THEN TIMESTAMP(DATE(COALESCE(TIMESTAMP(sfp.workorder_date_creation), sfp.demand_created_at)) + 1)
          ELSE COALESCE(TIMESTAMP(sfp.workorder_date_creation), sfp.demand_created_at)
        END AS date_creation_ref
    FROM {{ ref('fct_yuman__workorder_pricing') }} sfp
),
dates_range AS (
  SELECT
    ad.sfp_numero,
    ad.date_creation_initial,
    ad.date_fin,
    ad.date_creation_ref,
    date_jour
  FROM adjusted_dates ad,
  UNNEST(
    GENERATE_DATE_ARRAY(
      DATE(ad.date_creation_ref),
      DATE(ad.date_fin),
      INTERVAL 1 DAY
    )
  ) AS date_jour
),
jours_feries as (
select * from {{ ref('ref_general__feries_metropole') }}  
),
jours_ouvrables AS (
    -- Filtrer les jours ouvrés (exclure week-ends et jours fériés)
    select
    	dr.sfp_numero,
        dr.date_creation_initial,
        dr.date_fin,
        dr.date_creation_ref,
        dr.date_jour,
        jf.date_ferie
    FROM
        dates_range dr
    LEFT JOIN jours_feries jf ON dr.date_jour = jf.date_ferie
    WHERE
        EXTRACT(DAYOFWEEK FROM dr.date_jour) NOT IN (1, 7)-- Exclure samedi (7) et dimanche (1)
        AND jf.date_ferie IS null -- Exclure jours fériés
),
delai_calcul AS (
    -- Calculer le nombre total de jours ouvrés entre date_creation_ref et date_fin
    select
    	jo.sfp_numero,
        jo.date_creation_initial,
        jo.date_fin,
        jo.date_creation_ref,
        COUNT(jo.date_jour)-1 AS delai_jours_ouvres
    FROM
        jours_ouvrables jo
    GROUP BY
        jo.sfp_numero,jo.date_creation_initial, jo.date_fin, jo.date_creation_ref
),
-- Résultat final CTE
final_table as (
  select sfp.*,dc.date_creation_ref,dc.delai_jours_ouvres,
    (case when dc.delai_jours_ouvres = 0 then 'J+0,5'
    	  when dc.delai_jours_ouvres = 1 and extract(time from date_creation_ref) > '12:00:00' and extract(time from date_fin) < '12:00:00' then 'J+0,55'
    	  when dc.delai_jours_ouvres = 1 then 'J+1'
    	  when dc.delai_jours_ouvres = 2 then 'J+2'
    	  when dc.delai_jours_ouvres > 2 then 'J++'
    	  else 'ERREUR'
   	END) as type_delai
from {{ ref('fct_yuman__workorder_pricing') }} sfp
left join delai_calcul dc on dc.sfp_numero = sfp.workorder_id)
--where extract(month from sfp.date_done) = 11 and extract(year from sfp.date_done) = 2025)
-- SELECT REQUETE GLOBALE
select * from final_table
