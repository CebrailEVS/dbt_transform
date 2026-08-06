
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__clients_enrichis`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Clients Nespresso (tiers) enrichis : coordonn\u00e9es, contact, segmentation commerciale, et l'identifiant C4C le plus r\u00e9cent rattach\u00e9 au client.\n[COMMENT CONSTRUITE] stg_nesp_co__client enrichi de l'id C4C d\u00e9duit : on rapproche le tiers (third) du C4C le plus r\u00e9cent observ\u00e9 dans les opportunit\u00e9s et activit\u00e9s (d\u00e9duplication par date de r\u00e9f\u00e9rence).\n[GRAIN] 1 ligne par third (PK, cl\u00e9 m\u00e9tier client). ~21,3k lignes.\n[NOTES] Source commerciale Nespresso (WIP). third_c4c_id est NULL si aucun C4C n'a pu \u00eatre d\u00e9duit.\n"""
    )
    as (
      

with union_source as (

    -- =============================
    -- Opportunités
    -- =============================
    select
        safe_cast(regexp_extract(opp_id_compte, r'FR_(\d+)') as int64) as third,
        safe_cast(opp_id_client_c4c as int64) as id_c4c_client,
        opp_date_creation as reference_date
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__opportunites`
    where opp_id_compte is not null

    union all

    -- =============================
    -- Activités
    -- =============================
    select
        safe_cast(regexp_extract(act_id_nessoft, r'FR_(\d+)') as int64) as third,
        act_compte_id as id_c4c_client,
        act_date_debut as reference_date
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__activites`
    where act_id_nessoft is not null
),

dedup as (

    select
        third,
        id_c4c_client
    from (
        select
            third,
            id_c4c_client,
            row_number() over (
                partition by third
                order by reference_date desc nulls last
            ) as rn
        from union_source
        where third is not null
    )
    where rn = 1
)

select

    -- =============================
    -- Identifiants
    -- =============================
    c.third,
    c.third_name,
    c.third_status_descr,

    -- =============================
    -- Adresse
    -- =============================
    c.third_adr_ln1 as third_address_1,
    c.third_adr_ln2 as third_address_2,
    c.third_post_code,
    c.third_city,
    -- =============================
    -- Informations de contact
    -- =============================
    c.order_placer_name,
    c.order_placer_adr_ln1,
    c.order_placer_post_code,
    c.order_placer_city,
    c.order_placer_phone,

    -- =============================
    -- Segmentation & Métier
    -- =============================
    c.segmentation_hypercare,
    c.categorie_client as metier,
    c.region,
    c.secteur as third_secteur,
    case
        when c.secteur like '%FID%' then upper(c.region)
        else concat('RS ', c.categorie_client)
    end as third_groupe,

    -- =============================
    -- Informations légales
    -- =============================
    c.siret as third_siret,
    cast(c.club_dt_disp as date) as third_club_date,

    -- =============================
    -- ID C4C déduit
    -- =============================
    d.id_c4c_client as third_c4c_id

from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__client` as c

left join dedup as d
    on c.third = d.third
    );
  