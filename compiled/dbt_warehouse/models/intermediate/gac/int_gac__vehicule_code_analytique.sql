

with vehicule as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_gac__vehicule`
),

deduplicated as (
    select
        contrat_immatriculation_edi,
        -- Dans la source GAC, collaborateur_fonction_actuelle porte en réalité le code
        -- analytique (compta), pas un libellé de poste. On l'expose sous son vrai nom.
        collaborateur_fonction_actuelle as code_analytique,
        row_number() over (
            partition by contrat_immatriculation_edi
            order by contrat_type_date_d_effet desc, contrat_id_gac desc
        ) as rn
    from vehicule
    -- On écarte les immatriculations non réelles (refs internes GAC préfixées '#',
    -- véhicules non immatriculés) et les véhicules sans code analytique.
    where
        contrat_immatriculation_edi is not null
        and not starts_with(contrat_immatriculation_edi, '#')
        and collaborateur_fonction_actuelle is not null
)

select
    contrat_immatriculation_edi,
    code_analytique
from deduplicated
where rn = 1