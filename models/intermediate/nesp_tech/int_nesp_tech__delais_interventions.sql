{{
    config(
        materialized='table',
        description='Modèle intermediate construisant la clé de facturation des interventions techniques selon typologie, machine, mini-prev et zone montagne'
    )
}}

WITH inter AS (
    -- Préparation des interventions éligibles
    -- + définition de la date de référence pour le calcul des délais
    SELECT
        *,
        CASE
            -- Si pickup avant création, on prend création comme point de départ
            WHEN pickup_date < creation_date THEN creation_date
            ELSE pickup_date
        END AS date_creation_delai

    FROM {{ref('stg_nesp_tech__interventions') }}

    -- Filtrage des interventions traitées uniquement
    WHERE etat_intervention IN ('terminée signée', 'signature différée')
      AND agency IN ('evs idf', 'evs', 'evs paris', 'evs paris 2')
),

feries AS (
    -- Table de référence des jours fériés France métropole
    SELECT 
        CAST(date_ferie AS DATE) AS date_ferie
    FROM {{ ref('ref_general__feries_metropole') }}
),

exploded AS (
    -- Expansion journalière : 1 ligne par jour entre création et fin
    -- Permet un calcul précis des jours et heures ouvrées
    SELECT
        i.n_planning,
        i.type,
        i.code_machine,
        i.date_creation_delai,
        i.date_heure_debut,
        i.date_heure_fin,

        -- Date calendrier générée
        d AS cal_date,

        -- Indique si le jour est férié
        f.date_ferie

    FROM inter i

    CROSS JOIN UNNEST(
        GENERATE_DATE_ARRAY(
            LEAST(DATE(i.date_creation_delai), DATE(i.date_heure_debut)),
            DATE(i.date_heure_fin)
        )
    ) AS d

    LEFT JOIN feries f
        ON d = f.date_ferie
),

delais AS (
    -- Agrégation des délais par intervention
    SELECT
        n_planning,
        type,
        code_machine,

        /* ============================
           JOURS OUVRÉS AVANT DÉBUT
           ============================ */
        COUNTIF(
            EXTRACT(DAYOFWEEK FROM cal_date) NOT IN (1,7)
            AND date_ferie IS NULL
            AND cal_date <= DATE(date_heure_debut)
        ) AS delai_jours_debut,

        /* ============================
           JOURS OUVRÉS AVANT FIN
           ============================ */
        COUNTIF(
            EXTRACT(DAYOFWEEK FROM cal_date) NOT IN (1,7)
            AND date_ferie IS NULL
            AND cal_date <= DATE(date_heure_fin)
        ) AS delai_jours_fin,

        /* ============================
           HEURES OUVRÉES JUSQU’AU DÉBUT
           ============================ */
        SUM(
            CASE
                WHEN EXTRACT(DAYOFWEEK FROM cal_date) IN (1,7)
                     OR date_ferie IS NOT NULL
                     OR cal_date > DATE(date_heure_debut)
                THEN 0
                ELSE TIMESTAMP_DIFF(
                    LEAST(
                        date_heure_debut,
                        TIMESTAMP_ADD(TIMESTAMP(cal_date), INTERVAL 1 DAY)
                    ),
                    GREATEST(date_creation_delai, TIMESTAMP(cal_date)),
                    SECOND
                ) / 3600
            END
        ) AS delai_heures_debut,

        /* ============================
           HEURES OUVRÉES JUSQU’A LA FIN
           ============================ */
        SUM(
            CASE
                WHEN EXTRACT(DAYOFWEEK FROM cal_date) IN (1,7)
                     OR date_ferie IS NOT NULL
                     OR cal_date > DATE(date_heure_fin)
                THEN 0
                ELSE TIMESTAMP_DIFF(
                    LEAST(
                        date_heure_fin,
                        TIMESTAMP_ADD(TIMESTAMP(cal_date), INTERVAL 1 DAY)
                    ),
                    GREATEST(date_creation_delai, TIMESTAMP(cal_date)),
                    SECOND
                ) / 3600
            END
        ) AS delai_heures_fin

    FROM exploded
    GROUP BY n_planning, type, code_machine
),

final AS (
    -- Calcul des attributs métiers intermédiaires
    SELECT
        *,

        -- Délai total de traitement (jours ouvrés)
        delai_jours_fin - delai_jours_debut AS delai_traitement_jours,

        -- Catégorisation SLA fin
        CASE
            WHEN delai_jours_fin <= 1 THEN 'J+0'
            WHEN delai_jours_fin = 2 THEN 'J+1'
            WHEN delai_jours_fin = 3 THEN 'J+2'
            WHEN delai_jours_fin = 4 THEN 'J+3'
            ELSE 'J++'
        END AS type_delai_fin,

        -- Catégorisation SLA début
        CASE
            WHEN delai_jours_debut <= 1 THEN 'J+0'
            WHEN delai_jours_debut = 2 THEN 'J+1'
            WHEN delai_jours_debut = 3 THEN 'J+2'
            WHEN delai_jours_debut = 4 THEN 'J+3'
            ELSE 'J++'
        END AS type_delai_debut,

        -- Flag bonus (logique métier intermédiaire)
        CASE
            WHEN delai_jours_fin <= 2
                 AND type = '5'
                 AND code_machine NOT LIKE 'ag%'
            THEN TRUE
            ELSE FALSE
        END AS delai_bonus_bool,

        -- Montant bonus calculé
        CASE
            WHEN delai_jours_fin <= 2
                 AND type = '5'
                 AND code_machine NOT LIKE 'ag%'
            THEN 15
            ELSE 0
        END AS delai_bonus_valeur

    FROM delais
)

-- ============================
-- SORTIE INTERMEDIATE
-- ============================
SELECT
    -- Info Intervention
    n_planning,
    type,
    code_machine,
    -- Info delais debut (utile primes)
    delai_jours_debut,
    delai_heures_debut,
    type_delai_debut,
    -- Info delais fin (utiles facturation)
    delai_jours_fin,
    delai_heures_fin,
    type_delai_fin,
    delai_traitement_jours,
    -- Info Bonus Curative
    delai_bonus_bool,
    delai_bonus_valeur
FROM final
