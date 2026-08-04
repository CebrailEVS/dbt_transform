

-- Facturation retraitée (modèle « live », toujours à jour) : chaque intervention
-- de fct_technique__intervention, enrichie des décisions de retraitement saisies
-- par les managers dans l'app Suivi Tech (int_apptech__retraitements).
--
-- Grain : 1 ligne par key_inter (extension 1:1 du fait, pas de fan-out).
-- Les retraitements sont pivotés à 1 ligne / key_inter (une colonne par
-- (type, champ)), fusionnés colonne par colonne, puis appliqués au fait pour
-- produire des colonnes « effectives » (statut / technicien / délai post-
-- retraitement) que les marts Primes consomment directement.
--
-- Convention de nommage des colonnes de sortie :
--   - sans suffixe  = état BRUT du fait (avant retraitement, pour audit)
--   - suffixe _effectif = vérité POST-retraitement (consommée par les Primes)
--   - colonnes *_retraite / *_mee / *_modif / *_astreinte = valeurs de
--     retraitement fusionnées ou brutes par type (audit / débogage)
--
-- Deux colonnes logiques ont plus d'un type contributeur (cf. contrat DA) :
--   a_facturer_retraite  ← mee OU modif_intervention
--   tech_id_reel_retraite ← astreinte OU modif_intervention
-- Un test de collision (error) échoue si les deux contributeurs divergent.

with retraitements as (

    select * from `evs-datastack-prod`.`prod_intermediate`.`int_apptech__retraitements`
    -- key_inter NULL = src_inter non émis par l'app : jointure impossible, on écarte.
    where key_inter is not null

),

-- Pivot : 1 ligne par key_inter, une colonne par (type_retraitement, champ).
pivoted as (

    select
        key_inter,

        -- Décision de facturation (domaines source distincts, normalisés plus bas)
        max(case when type_retraitement = 'mee' then a_facturer end) as a_facturer_mee,
        max(case when type_retraitement = 'modif_intervention' then a_facturer end) as a_facturer_modif,

        -- Technicien réel (astreinte ou modif_intervention)
        max(case when type_retraitement = 'astreinte' then tech_id_reel end) as tech_id_reel_astreinte,
        max(case when type_retraitement = 'astreinte' then tech_yuman_id_reel end) as tech_yuman_id_reel_astreinte,
        max(case when type_retraitement = 'modif_intervention' then tech_id_reel end) as tech_id_reel_modif,
        max(case when type_retraitement = 'modif_intervention' then tech_yuman_id_reel end) as tech_yuman_id_reel_modif,

        -- Délais forcés (curative uniquement)
        max(case when type_retraitement = 'curative' then delai_tech_force end) as delai_tech_force,
        max(case when type_retraitement = 'curative' then delai_partenaire_force end) as delai_partenaire_force,

        -- Flags consommés par les Primes (pause / aguila)
        max(case when type_retraitement = 'pause' then doubler_prime end) as doubler_prime,
        max(case when type_retraitement = 'aguila' then convertir_code_5 end) as convertir_code_5,

        -- Type de modification (modif_intervention)
        max(case when type_retraitement = 'modif_intervention' then type_modif end) as type_modif,

        -- Traçabilité
        string_agg(distinct type_retraitement order by type_retraitement) as types_retraitement,
        string_agg(
            case when commentaire is not null then concat(type_retraitement, ': ', commentaire) end
            order by type_retraitement
        ) as commentaire_retraitement,
        max(extracted_at) as retraite_extracted_at

    from retraitements
    group by key_inter

),

-- Fusion colonne par colonne + normalisation de la décision de facturation.
-- mee : OUI/NON tel quel. modif : 'NOT VALIDATED' => 'NON', sinon pas d'avis.
merged as (

    select
        key_inter,

        coalesce(
            case a_facturer_mee when 'OUI' then 'OUI' when 'NON' then 'NON' end,
            case when a_facturer_modif = 'NOT VALIDATED' then 'NON' end
        ) as a_facturer_retraite,
        coalesce(tech_id_reel_astreinte, tech_id_reel_modif) as tech_id_reel_retraite,
        coalesce(tech_yuman_id_reel_astreinte, tech_yuman_id_reel_modif) as tech_yuman_id_reel_retraite,
        delai_tech_force,
        delai_partenaire_force,
        doubler_prime,
        convertir_code_5,
        type_modif,

        -- Brut par type (audit / débogage)
        a_facturer_mee,
        a_facturer_modif,
        tech_id_reel_astreinte,
        tech_yuman_id_reel_astreinte,
        tech_id_reel_modif,
        tech_yuman_id_reel_modif,

        types_retraitement,
        commentaire_retraitement,
        retraite_extracted_at

    from pivoted

),

-- Application des retraitements au fait : colonnes brutes (fait) + colonnes
-- effectives (post-retraitement). Le technicien effectif n'est ici qu'un ID ;
-- son nom/secteur/nomad_id sont re-mappés dans le SELECT final.
effective as (

    select
        -- Grain + identité (repris du fait)
        f.key_inter,
        f.src_inter,
        f.partenaire,
        f.intervention_id,
        f.numero_pu,

        -- Attributs intervention (fait)
        f.intervention_statut,
        f.statut_facturation,
        f.categorie_machine,
        f.machine_clean,
        f.type_intervention,
        f.num_serie_machine,
        f.tech_id,
        f.client_id,
        f.client_nom,
        f.date_creation,
        f.date_debut,
        f.date_fin,
        f.duree_inter_minutes,
        f.key_factu,
        f.code_postal,
        f.consignes,
        f.commentaire_tech,
        f.prod,
        f.montant,
        f.bonus_bool,
        f.montant_avec_bonus,
        f.delai_heures_debut,
        f.delai_heures_fin,
        f.delai_tech,
        f.delai_partenaire,
        f.flag_montagne_prime,
        f.flag_paris_intramuros,
        f.flag_hors_delai_tech,
        f.tech_yuman_id,
        f.tech_nomad_id,
        f.tech_nom,
        f.tech_secteur,
        f.alias_obj_type_inter,
        f.alias_obj_type_machine,
        f.alias_obj_grp_machine,

        -- Colonnes effectives (fait après application des retraitements)
        case
            when m.a_facturer_retraite = 'OUI' then 'VALIDATED'
            when m.a_facturer_retraite = 'NON' then 'NOT VALIDATED'
            else f.statut_facturation
        end as statut_facturation_effectif,
        -- tech_id_effectif reste dans l'espace d'ID NATIF de la source :
        -- nomad (tec-xxxx) côté NESP, id Yuman numérique côté YUMAN. L'app écrit
        -- le technicien réel en nomad (tech_id_reel) ET en id Yuman
        -- (tech_yuman_id_reel), donc on prend le bon selon src_inter.
        case
            when f.src_inter = 'NESP' then coalesce(m.tech_id_reel_retraite, f.tech_id)
            when f.src_inter = 'YUMAN' then coalesce(cast(m.tech_yuman_id_reel_retraite as string), f.tech_id)
        end as tech_id_effectif,
        coalesce(m.tech_yuman_id_reel_retraite, f.tech_yuman_id) as tech_yuman_id_effectif,
        coalesce(m.delai_tech_force, f.delai_tech) as delai_tech_effectif,
        coalesce(m.delai_partenaire_force, f.delai_partenaire) as delai_partenaire_effectif,
        -- Conversion code 5 (aguila) : force le code de tête de key_factu de 1
        -- à 5, ce qui change la clé de facturation (donc tarif + prod, re-lookés
        -- dans le SELECT final). Côté app, convertir_code_5='OUI' n'est proposé
        -- que sur des interventions key_factu '1 - ... Aguila', donc le swap est sûr.
        case
            when m.convertir_code_5 = 'OUI' then regexp_replace(f.key_factu, r'^1 ', '5 ')
            else f.key_factu
        end as key_factu_effectif,

        -- Décisions de retraitement fusionnées
        m.a_facturer_retraite,
        m.tech_id_reel_retraite,
        m.tech_yuman_id_reel_retraite,
        m.delai_tech_force,
        m.delai_partenaire_force,
        m.doubler_prime,
        m.convertir_code_5,
        m.type_modif,

        -- Traçabilité
        coalesce(m.types_retraitement is not null, false) as has_retraitement,
        m.types_retraitement,
        m.commentaire_retraitement,

        -- Brut par type (audit)
        m.a_facturer_mee,
        m.a_facturer_modif,
        m.tech_id_reel_astreinte,
        m.tech_yuman_id_reel_astreinte,
        m.tech_id_reel_modif,
        m.tech_yuman_id_reel_modif,

        -- Métadonnées
        m.retraite_extracted_at

    from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention` as f
    left join merged as m on f.key_inter = m.key_inter

)

select
    e.*,

    -- Technicien effectif complet : re-mapping nom/secteur/nomad_id sur l'id
    -- Yuman effectif (tech_yuman_id_effectif), clé universelle NESP+YUMAN
    -- (l'app résout toujours l'id Yuman du technicien réel). Les colonnes
    -- tech_nom/tech_secteur/tech_nomad_id ci-dessus restent le PLANIFIÉ (audit).
    tech_eff.user_name as tech_nom_effectif,
    tech_eff.user_secteur as tech_secteur_effectif,
    tech_eff.nomad_id as tech_nomad_id_effectif,

    -- Flag hors-délai recalculé sur le délai effectif (même règle que le fait).
    case
        when e.delai_tech_effectif in ('J++', 'J+3') and lower(e.key_factu) like '%curative%'
            then 1
        else 0
    end as flag_hors_delai_tech_effectif,

    -- Facturation effective : si conversion code 5, tarif/prod re-lookés sur la
    -- clé effective ; sinon valeurs du fait. (kf_eff ne résout que les clés
    -- NESP ; le case garde le montant du fait pour tout le reste, dont YUMAN.)
    case when e.convertir_code_5 = 'OUI' then kf_eff.prod_factu else e.prod end as prod_effectif,
    case when e.convertir_code_5 = 'OUI' then kf_eff.tarif_factu else e.montant end as montant_effectif
from effective as e
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__users` as tech_eff
    on e.tech_yuman_id_effectif = tech_eff.user_id
left join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation` as kf_eff
    on e.key_factu_effectif = kf_eff.key_ref_inter