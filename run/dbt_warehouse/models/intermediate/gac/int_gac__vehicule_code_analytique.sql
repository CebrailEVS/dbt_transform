
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_gac__vehicule_code_analytique`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Table de correspondance immatriculation \u2192 code analytique (compta) du v\u00e9hicule. Brique r\u00e9utilisable pour ventiler analytiquement un v\u00e9hicule identifi\u00e9 par son immat (\u00e9quivalent d\u00e9riv\u00e9 de la source GAC du mapping compta mapping_immat_code.csv).\n[COMMENT CONSTRUITE] D\u00e9dup de stg_gac__vehicule (1 ligne par p\u00e9riode de contrat type) \u00e0 1 ligne par immatriculation, en gardant la p\u00e9riode de contrat type la plus r\u00e9cente (row_number partition by immat order by contrat_type_date_d_effet desc, contrat_id_gac desc). Le code analytique provient de la colonne source collaborateur_fonction_actuelle, dont le nom est trompeur : elle porte en r\u00e9alit\u00e9 le code analytique (ex. SAVLYOTECH, COMLYOHOR), pas un libell\u00e9 de poste. Exclut les immatriculations non r\u00e9elles (refs internes GAC pr\u00e9fix\u00e9es '#') et les v\u00e9hicules sans code analytique (valeur NULL).\n[GRAIN] 1 ligne par immatriculation (contrat_immatriculation_edi). Cl\u00e9 strictement unique. ~126 immatriculations renseign\u00e9es (sur 271 immats r\u00e9els : ~145 v\u00e9hicules de pool / non affect\u00e9s, sans code analytique, sont volontairement exclus).\n[NOTES] Le code analytique est constant sur toutes les lignes d'un m\u00eame immat dans la source : la d\u00e9dup ne fait que choisir une ligne repr\u00e9sentative, sans arbitrage de valeur.\n"""
    )
    as (
      

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
    );
  