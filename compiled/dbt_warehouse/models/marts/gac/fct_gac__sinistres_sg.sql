

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
    '1c81c2d2-420f-4dcf-bea0-fd653c2c4dd4' as dbt_invocation_id

from `evs-datastack-prod`.`prod_staging`.`stg_gac__sinistres_sg`