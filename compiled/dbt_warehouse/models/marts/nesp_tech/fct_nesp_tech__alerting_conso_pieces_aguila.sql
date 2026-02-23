

-- ============================================================
-- CTE 1 : Agrégation des articles consommés par intervention
-- Objectif :
--   - Normaliser le grain : 1 ligne = 1 article / intervention
--   - Éviter toute duplication en aval
-- ============================================================
with articles as (

    select
        n_planning,                 -- Identifiant intervention (clé métier)
        code_article,               -- Code article consommé (déjà normalisé en minuscules)
        SUM(quantite_article) as qty -- Quantité totale consommée pour cet article
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup`
    group by n_planning, code_article

),

-- ============================================================
-- CTE 2 : Pivot logique des consommations d’articles
-- Objectif :
--   - Transformer les articles en indicateurs analytiques
--   - Pré-calculer tous les flags métier (BigQuery-friendly)
-- ============================================================
articles_pivot as (

    select
        n_planning,

        -- Présence PREV / MINIPREV
        MAX(COALESCE(code_article in ('prev', 'miniprev'), false)) as has_prev_or_miniprev,

        -- Présence spécifique MINIPREV / PREV
        MAX(COALESCE(code_article = 'miniprev', false)) as has_miniprev,
        MAX(COALESCE(code_article = 'prev', false)) as has_prev,

        -- Présence filtre obligatoire
        MAX(COALESCE(code_article = 'everpurexl', false)) as has_filtre,

        -- Comptage des kits consommés
        COUNTIF(code_article in ('kitcomptp126', 'tp126617', 'tp130174', 'tp125377')) as nb_kits,

        -- Présence d’au moins un kit
        MAX(COALESCE(code_article in ('kitcomptp126', 'tp126617', 'tp130174', 'tp125377'), false))
            as has_kit,

        -- Cohérence kit vs machine
        MAX(COALESCE(code_article in ('tp126617', 'kitcomptp126'), false)) as kit_aguila2_ok,
        MAX(COALESCE(code_article in ('tp130174', 'tp125377'), false)) as kit_aguila4_ok,

        -- Quantités pièces hors kit
        SUM(IF(code_article = 'tp126015', qty, 0)) as qty_126015,
        SUM(IF(code_article = 'tp120555', qty, 0)) as qty_120555,
        SUM(IF(code_article = 'tp120257', qty, 0)) as qty_120257,

        -- Détection consommation hors kit
        MAX(COALESCE(code_article in ('tp126015', 'tp120555', 'tp120257'), false))
            as has_hors_kit_parts

    from articles
    group by n_planning

),

-- ============================================================
-- CTE 3 : Application des règles métier qualité
-- Objectif :
--   - Générer les alertes analytiques par intervention
--   - Centraliser toute la logique métier dans dbt
-- ============================================================
final as (

    select
        i.n_planning,
        CAST(i.date_heure_fin as DATE) as date_fin, -- Date analytique standardisée
        i.intervention_type,
        i.consignes,

        -- Normalisation machine via référentiel
        r.categorie_machine,
        r.machine_clean,

        -- ====================================================
        -- Règles métier qualité
        -- ====================================================

        -- Vérification nomenclature PREV / MINIPREV
        case
            when
                intervention_type = '2'
                and not has_prev_or_miniprev
                then 'NOMENCLATURE'
        end as alt_nomenclature,

        -- Cohérence consigne vs articles consommés
        case
            when
                intervention_type = '2'
                and LOWER(consignes) like 'mini-pr%'
                and not has_miniprev
                then 'CONSIGNE'

            when
                intervention_type = '2'
                and LOWER(consignes) not like 'mini-pr%'
                and not has_prev
                then 'CONSIGNE'
        end as alt_coherence_consigne,

        -- PREV consommé → kit obligatoire
        case
            when has_prev and not has_kit
                then 'PAS DE KIT'
        end as alt_prevcompletekit,

        -- Cohérence kit vs type machine
        case
            when has_kit and machine_clean = 'Aguila 2' and not kit_aguila2_ok
                then 'KIT INCOHERENT'

            when has_kit and machine_clean = 'Aguila 4' and not kit_aguila4_ok
                then 'KIT INCOHERENT'
        end as alt_kit_typemachine,

        -- Filtre obligatoire sur préventives
        case
            when
                intervention_type = '2'
                and not has_filtre
                then 'PAS DE FILTRE'
        end as alt_filtre_consomme,

        -- Détection consommation multiple de kits
        case
            when nb_kits > 1
                then 'PLUSIEURS KITS'
        end as alt_nb_kits,

        -- Contrôles hors kit
        case
            when has_kit and has_hors_kit_parts
                then 'PROBLEME KIT + AUTRES'

            when
                not has_kit and machine_clean = 'Aguila 2'
                and (qty_126015 != 2 or qty_120555 != 2 or qty_120257 != 1)
                then 'QUANTITES INCOHERENTES'

            when
                not has_kit and machine_clean = 'Aguila 4'
                and (qty_126015 != 4 or qty_120555 != 4 or qty_120257 != 1)
                then 'QUANTITES INCOHERENTES'
        end as alt_conso_hors_kit

    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup` as i

    -- Jointure référentiel machines (clé de normalisation)
    left join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__machines_clean` as r
        on i.nom_machine = LOWER(r.nom_machine)

    -- Jointure indicateurs de consommation
    left join articles_pivot as ap
        on i.n_planning = ap.n_planning

    -- Filtrage analytique ciblé (Aguila uniquement)
    where
        r.categorie_machine like '%AGUILA%'
        and intervention_type = '2'
        and etat_intervention not in ('annulée', 'mise en échec')

)

-- ============================================================
-- Sélection finale modèle analytics
-- Objectif :
--   - Exposer dataset BI prêt Power BI / Looker / reporting
-- ============================================================
select
    -- Informartion sur l'intervention
    n_planning,
    date_fin,
    intervention_type,
    consignes,
    categorie_machine,
    machine_clean,

    -- Flags Alertes Conso
    alt_nomenclature,
    alt_coherence_consigne,
    alt_prevcompletekit,
    alt_kit_typemachine,
    alt_filtre_consomme,
    alt_nb_kits,
    alt_conso_hors_kit,

    -- Compilation des alertes (équivalent concat_ws PostgreSQL)
    ARRAY_TO_STRING(
        ARRAY(
            select alert
            from
                UNNEST([
                    alt_nomenclature,
                    alt_coherence_consigne,
                    alt_prevcompletekit,
                    alt_kit_typemachine,
                    alt_filtre_consomme,
                    alt_nb_kits,
                    alt_conso_hors_kit
                ]) as alert
            where alert is not null
        ),
        '-'
    ) as alerte_compile,

    -- Métadonnées dbt (audit & lineage)
    CURRENT_TIMESTAMP() as dbt_updated_at,
    '03229da1-54a3-41e2-af89-b274a343e19d' as dbt_invocation_id  -- noqa: TMP

from final