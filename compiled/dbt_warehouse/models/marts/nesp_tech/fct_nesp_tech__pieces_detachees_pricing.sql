

with inters as (
    select
        n_planning,
        cast(date_heure_fin as date) as date_fin
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__interventions`
    where
        etat_intervention in ('terminée signée', 'signature différée')
        and agency in ('evs', 'evs paris', 'evs idf', 'evs paris 2')
),

final as (
    select
        i.n_planning,
        i.date_fin,
        extract(year from i.date_fin) as annee,
        extract(month from i.date_fin) as mois,
        extract(day from i.date_fin) as jour,
        art_conso.n_tech,
        art_conso.nom_tech,
        art_conso.prenom_tech,
        art_pricing.article_ref_nomad as piece_ref_nomad,
        art_pricing.article_desc as piece_desc,
        art_conso.quantite_article as piece_quantite,
        art_pricing.article_prix_unitaire as piece_prix_unitaire,
        (art_conso.quantite_article * art_pricing.article_prix_unitaire) as montant_total
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__articles` as art_conso
    left join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix` as art_pricing
        on art_conso.code_article = lower(art_pricing.article_ref_nomad)
    inner join inters as i
        on art_conso.n_planning = i.n_planning
)

select
    -- Info Interventions et Technicien
    n_planning,
    date_fin,
    n_tech,
    nom_tech,
    prenom_tech,

    -- Info Pieces detachees
    piece_ref_nomad,
    piece_desc,
    piece_prix_unitaire,
    piece_quantite,

    -- Montant a facturer
    montant_total,

    -- Metadonnees dbt
    current_timestamp() as dbt_updated_at,
    '03229da1-54a3-41e2-af89-b274a343e19d' as dbt_invocation_id

from final