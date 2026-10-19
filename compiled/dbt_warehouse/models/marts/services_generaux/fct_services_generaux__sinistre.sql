

select
    -- Infos Sinistre
    n_de_sinistre,
    date(date_sinistre) as date_sinistre,
    date_de_creation,
    circonstance,
    tiers,
    resp,
    cloture,

    -- Infos Vehicule
    immat,
    nom,
    prenom,
    statut_actuel,
    genre_fiscal,
    reference_gac,
    entite_entite_2,
    entite_entite_3,

    -- Couts
    centre_de_couts,
    cout_assureur,
    auto_assurance,
    franchise,
    cout_global,
    cout_client,

    -- Metadonnees dbt
    current_timestamp() as dbt_updated_at,
    '01a15370-5573-72f0-ba68-173f71e4d3fd' as dbt_invocation_id

from `evs-datastack-prod`.`prod_staging`.`stg_gac__sinistres`