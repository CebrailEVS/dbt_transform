

WITH inter AS (
    -- Préparation des interventions :
    -- - Ajout du flag mini_prev_bool
    -- - Calcul du département site à partir du code postal
    SELECT
        int.*,

        -- Flag indiquant si un article "miniprev" est présent
        IF(art.code_article IS NULL, FALSE, TRUE) AS mini_prev_bool,

        -- Extraction du département depuis le code postal
        CAST(
            SUBSTR(
                LPAD(CAST(code_postal_site AS STRING), 5, '0'),
                1,
                2
            ) AS INT64
        ) AS dpt_site

    FROM `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__interventions` int

    LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__articles` art
        ON int.n_planning = art.n_planning
       AND art.code_article = 'miniprev'

    -- Filtrage temporel et métier
    WHERE date_heure_fin BETWEEN '2025-12-01 00:00:01' AND '2025-12-31 23:59:59'
      AND etat_intervention IN ('terminée signée','signature différée')
),

machines_clean AS (
    -- Référentiel machines normalisées
    SELECT
        LOWER(nom_machine) AS nom_machine,
        categorie_machine,
        machine_clean
    FROM `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__machines_clean`
),

departements_montagne AS (
    -- Liste des départements facturés en zone montagne
    SELECT
        CAST(dpt AS INT64) AS dpt
    FROM `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__dpts_montagne_factu`
    WHERE montagne = 1
),

ref_type_inter AS (
    -- Référentiel de typologie d’intervention (clé métier)
    SELECT
        CAST(type_code AS STRING) AS type_code,
        CAST(repair_code_1 AS STRING) AS repair_code_1,
        CAST(failure_code AS STRING) AS failure_code,
        mini_prev_bool,
        type_machine,
        type_inter_libelle
    FROM `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_type_inter`
),

joined AS (
    -- Jointure centrale : attribution d’un type d’intervention facturable
    SELECT
        ma.machine_clean,
        r.type_inter_libelle,
        r.type_code,

        -- Ajout d’un suffixe Montagne si applicable
        IF(dm.dpt IS NOT NULL, ' - Montagne', '') AS montagne_factu,

        -- Construction de la clé de facturation
        CONCAT(
            r.type_code,
            ' - ',
            r.type_inter_libelle,
            ' - ',
            ma.machine_clean,
            IF(dm.dpt IS NOT NULL, ' - Montagne', '')
        ) AS key_factu,

        i.n_planning,
        i.type,
        i.code_machine,
        i.nom_machine,
        i.repair_code_1,
        i.failure_code,
        i.mini_prev_bool,
        i.dpt_site,
        i.date_heure_fin,

        -- Priorisation des règles de mapping via scoring
        ROW_NUMBER() OVER (
            PARTITION BY i.n_planning
            ORDER BY
                (CASE WHEN r.repair_code_1 IS NOT NULL THEN 16 ELSE 0 END) +
                (CASE WHEN r.failure_code  IS NOT NULL THEN 8  ELSE 0 END) +
                (CASE WHEN r.mini_prev_bool IS NOT NULL THEN 4 ELSE 0 END) +
                (CASE WHEN r.type_machine  IS NOT NULL THEN 2 ELSE 0 END)
            DESC
        ) AS rn

    FROM inter i

    LEFT JOIN machines_clean ma
        ON LOWER(i.nom_machine) = ma.nom_machine

    LEFT JOIN ref_type_inter r
        ON i.type = r.type_code
       AND (i.repair_code_1 = r.repair_code_1 OR r.repair_code_1 IS NULL)
       AND (i.failure_code = r.failure_code OR r.failure_code IS NULL)
       AND (i.mini_prev_bool = r.mini_prev_bool OR r.mini_prev_bool IS NULL)
       AND (ma.categorie_machine = r.type_machine OR r.type_machine IS NULL)

    LEFT JOIN departements_montagne dm
        ON i.dpt_site = dm.dpt
),

final AS (
    -- Jointure avec la table de correspondance facturation
    SELECT
        j.n_planning,
        j.type_inter_libelle,
        j.key_factu,
        kf.prod_factu,
        kf.tarif_factu
    FROM joined j

    LEFT JOIN `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation` kf
        ON kf.key_ref_inter = j.key_factu

    -- Sélection de la règle la plus prioritaire
    WHERE j.rn = 1
)

-- ============================
-- SELECT FINAL EXPLICITE
-- ============================
SELECT
    n_planning,
    type_inter_libelle,
    key_factu,
    prod_factu,
    tarif_factu
FROM final