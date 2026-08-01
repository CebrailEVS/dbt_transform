

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_comptea`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Clé métier. Sage déclare l'unicité sur (N_Analytique, CA_Num), mais
        -- n_analytique ne porte qu'UNE valeur ici — il n'y a qu'un plan analytique.
        -- ca_num est donc unique à lui seul : mesuré 309 valeurs pour 309 lignes.
        n_analytique,
        ca_num,
        ca_intitule,

        -- Caractérisation de la section
        ca_type,
        ca_classement,
        ca_statut,
        ca_sommeil,
        ca_domaine,
        ca_mode_facturation,
        co_no,

        -- Cycle de vie de l'affaire
        ca_date_creation_affaire,
        ca_date_accept_affaire,
        ca_date_debut_affaire,
        ca_date_fin_affaire,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
-- Défensif : le raw est un snapshot complet, donc ca_num y est déjà unique.
qualify row_number() over (
    partition by ca_num
    order by updated_at desc, cb_marq desc
) = 1