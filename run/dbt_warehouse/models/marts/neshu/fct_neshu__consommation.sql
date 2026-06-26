
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_neshu__consommation`
      
    partition by consumption_date
    cluster by company_id, product_id, device_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Consommations produits clients Neshu, consolid\u00e9es depuis 3 sources op\u00e9rationnelles.\n[COMMENT CONSTRUITE] Union de 3 flux : t\u00e9l\u00e9m\u00e9trie machines (int_oracle_neshu__telemetry_tasks), chargements machines (int_oracle_neshu__chargement_tasks), livraisons directes (int_oracle_neshu__livraison_tasks). Enrichi par dim_neshu__company, dim_neshu__device, dim_neshu__product. R\u00e8gles m\u00e9tier : (a) multiplicateurs de conditionnement (rame/bo\u00eete/carton) via le seed ref_oracle_neshu__product_packaging (cl\u00e9 product_id) appliqu\u00e9s aux flux chargement et livraison ; (b) arbitrage t\u00e9l\u00e9m\u00e9trie vs chargement port\u00e9 par la r\u00e8gle g\u00e9n\u00e9rale gratuit/payant (machines gratuites NESPRESSO/NESTLE/ANIMO compt\u00e9es via chargement, payantes via t\u00e9l\u00e9m\u00e9trie ; chocolats et accessoires toujours via chargement) ; (c) cas particuliers (machine/client) d\u00e9rogeant \u00e0 cette r\u00e8gle externalis\u00e9s dans le seed ref_oracle_neshu__consommation_source_override. La livraison n'est jamais arbitr\u00e9e. location='LIVRAISON' et device_*='LIVRAISON' pour les flux livraison directe (sans machine).\n[GRAIN] 1 ligne par (company_id, device_id, product_id, location_id, location, consumption_date, data_source).\n[NOTES] data_source \u2208 {TELEMETRIE, CHARGEMENT, LIVRAISON}. device_id NULL uniquement si data_source='LIVRAISON' (invariant test\u00e9). Overrides d'arbitrage : voir ref_oracle_neshu__consommation_source_override (mode redirection cibl\u00e9e vs takeover machine via is_takeover). Multiplicateurs : voir ref_oracle_neshu__product_packaging ; le test assert_neshu__packaging_seed_complete alerte si un produit conditionn\u00e9 manque au seed.\n"""
    )
    as (
      

with product_packaging as (
    -- Multiplicateur de conditionnement par produit (rame, boîte, carton...).
    -- Externalisé en seed : un produit absent vaut 1 (unité = unité).
    select
        product_id,
        units_per_pack
    from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__product_packaging`
),

source_override as (
    -- Cas particuliers d'arbitrage (machine/client) externalisés en seed.
    -- Colonnes vides = wildcard ("tous").
    select
        nullif(company_code, '') as company_code,
        nullif(device_serial_number, '') as device_serial_number,
        nullif(product_type, '') as product_type,
        forced_source,
        is_takeover,
        coalesce(date_from, date '0001-01-01') as date_from,
        coalesce(date_to, date '9999-12-31') as date_to
    from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__consommation_source_override`
),

telemetry_data as (
    select
        -- IDs
        t.company_id,
        t.device_id,
        t.location_id,
        t.product_id,

        -- Company
        c.company_code,
        c.company_name,

        -- Localisation
        coalesce(nullif(t.task_location_info, ''), d.device_location) as location,

        -- Machine
        d.device_code as device_serial_number,
        d.device_name,
        d.device_brand,
        d.device_economic_model,

        -- Produit
        p.product_name,
        p.product_brand,
        p.product_family,
        p.product_group,
        p.product_type,

        -- Contexte
        date(t.task_start_date) as consumption_date,
        'TELEMETRIE' as data_source,

        -- Mesure
        sum(t.telemetry_quantity) as quantity

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__telemetry_tasks` as t
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__device` as d
        on t.device_id = d.device_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as p
        on t.product_id = p.product_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__company` as c
        on t.company_id = c.company_id
    group by
        t.company_id, t.device_id, t.location_id, t.product_id,
        c.company_code, c.company_name,
        coalesce(nullif(t.task_location_info, ''), d.device_location),
        d.device_code, d.device_name, d.device_brand,
        d.device_economic_model,
        p.product_name, p.product_brand, p.product_family,
        p.product_group, p.product_type,
        date(t.task_start_date)
),

chargement_data as (
    select
        -- IDs
        l.company_id,
        l.device_id,
        l.location_id,
        l.product_id,

        -- Company
        c.company_code,
        c.company_name,

        -- Localisation
        coalesce(nullif(l.task_location_info, ''), d.device_location) as location,

        -- Machine
        d.device_code as device_serial_number,
        d.device_name,
        d.device_brand,
        d.device_economic_model,

        -- Produit
        p.product_name,
        p.product_brand,
        p.product_family,
        p.product_group,
        p.product_type,

        -- Contexte
        date(l.task_start_date) as consumption_date,
        'CHARGEMENT' as data_source,

        -- Quantité (ajustée selon le conditionnement du produit, cf. seed)
        sum(l.load_quantity * coalesce(pk.units_per_pack, 1)) as quantity

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks` as l
    inner join `evs-datastack-prod`.`prod_marts`.`dim_neshu__device` as d
        on l.device_id = d.device_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as p
        on l.product_id = p.product_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__company` as c
        on l.company_id = c.company_id
    left join product_packaging as pk
        on l.product_id = pk.product_id
    where l.task_status_code in ('FAIT', 'VALIDE')
    group by
        l.company_id, l.device_id, l.location_id, l.product_id,
        coalesce(nullif(l.task_location_info, ''), d.device_location),
        c.company_code, c.company_name,
        d.device_code, d.device_name, d.device_brand,
        d.device_economic_model,
        p.product_name, p.product_brand, p.product_family,
        p.product_group, p.product_type,
        date(l.task_start_date)
),

livraison_data as (
    select
        -- IDs
        lt.company_id,
        null as device_id,
        null as location_id,
        lt.product_id,

        -- Company
        c.company_code,
        c.company_name,

        -- Localisation (fixe)
        'LIVRAISON' as location,

        -- Machine (fixe)
        'LIVRAISON' as device_serial_number,
        'LIVRAISON' as device_name,
        'LIVRAISON' as device_brand,
        'LIVRAISON' as device_economic_model,

        -- Produit
        p.product_name,
        p.product_brand,
        p.product_family,
        p.product_group,
        p.product_type,

        -- Contexte
        date(lt.task_start_date) as consumption_date,
        'LIVRAISON' as data_source,

        -- Quantité (ajustée selon le conditionnement du produit, cf. seed)
        sum(lt.quantity * coalesce(pk.units_per_pack, 1)) as quantity

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__livraison_tasks` as lt
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as p
        on lt.product_id = p.product_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__company` as c
        on lt.company_id = c.company_id
    left join product_packaging as pk
        on lt.product_id = pk.product_id
    where
        lt.task_status_code in ('FAIT', 'VALIDE')
        and p.product_type in (
            'THE', 'CAFE CAPS', 'CHOCOLATS VAN HOUTEN',
            'BOISSONS GOURMANDES', 'ACCESSOIRES'
        )
    group by
        lt.company_id, lt.product_id,
        p.product_name, p.product_brand, p.product_family,
        p.product_group, p.product_type,
        c.company_code, c.company_name,
        date(lt.task_start_date)
),

-- Arbitrage des sources de consommation (télémétrie vs chargement).
-- Les cas particuliers (machine/client) sont externalisés dans le seed
-- source_override ; la règle générale ci-dessous ne porte que le principe
-- gratuit/payant. La livraison n'est jamais arbitrée (toujours conservée).
arbitrage_input as (
    select * from telemetry_data
    union all
    select * from chargement_data
),

-- Flags d'arbitrage calculés par cross join sur le seed (petit, qqs lignes)
-- + agrégation max(case). On évite les EXISTS corrélés que BigQuery réécrit
-- en SEMI/ANTI JOIN (lesquels exigent une égalité, impossible avec nos wildcards).
arbitrage_flagged as (
    select
        a.company_id,
        a.device_id,
        a.location_id,
        a.product_id,
        a.company_code,
        a.company_name,
        a.location,
        a.device_serial_number,
        a.device_name,
        a.device_brand,
        a.device_economic_model,
        a.product_name,
        a.product_brand,
        a.product_family,
        a.product_group,
        a.product_type,
        a.consumption_date,
        a.data_source,
        a.quantity,
        -- Takeover : un override cible explicitement cette machine. Elle est
        -- alors entièrement pilotée par le seed (toute ligne non reprise écartée).
        max(
            case
                when
                    o.is_takeover
                    and o.device_serial_number = a.device_serial_number
                    and o.company_code = a.company_code
                    then 1
                else 0
            end
        ) = 1 as is_machine_takeover,
        -- La ligne est-elle ciblée par un override (company/machine/type/fenêtre) ?
        max(
            case
                when
                    (o.company_code is null or o.company_code = a.company_code)
                    and (
                        o.device_serial_number is null
                        or o.device_serial_number = a.device_serial_number
                    )
                    and (o.product_type is null or o.product_type = a.product_type)
                    and a.consumption_date >= o.date_from
                    and a.consumption_date < o.date_to
                    then 1
                else 0
            end
        ) = 1 as is_claimed,
        -- La source de la ligne est-elle celle imposée par un override la ciblant ?
        max(
            case
                when
                    (o.company_code is null or o.company_code = a.company_code)
                    and (
                        o.device_serial_number is null
                        or o.device_serial_number = a.device_serial_number
                    )
                    and (o.product_type is null or o.product_type = a.product_type)
                    and a.consumption_date >= o.date_from
                    and a.consumption_date < o.date_to
                    and o.forced_source = a.data_source
                    then 1
                else 0
            end
        ) = 1 as kept_by_override
    from arbitrage_input as a
    cross join source_override as o
    group by
        a.company_id, a.device_id, a.location_id, a.product_id,
        a.company_code, a.company_name, a.location,
        a.device_serial_number, a.device_name, a.device_brand,
        a.device_economic_model, a.product_name, a.product_brand,
        a.product_family, a.product_group, a.product_type,
        a.consumption_date, a.data_source, a.quantity
),

-- 1) Lignes gérées par un override : on ne garde que la source imposée
override_data as (
    select * except (is_machine_takeover, is_claimed, kept_by_override)
    from arbitrage_flagged
    where
        (is_claimed or is_machine_takeover)
        and kept_by_override
),

-- 2) Lignes non gérées : règle générale (principe gratuit/payant)
general_data as (
    select * except (is_machine_takeover, is_claimed, kept_by_override)
    from arbitrage_flagged
    where
        not is_claimed
        and not is_machine_takeover
        and (
            -- TELEMETRIE par défaut (machines payantes/participatives)
            (
                data_source = 'TELEMETRIE'
                and product_type in (
                    'BOISSONS GOURMANDES', 'CAFE CAPS', 'CAFENOIR', 'INDEFINI',
                    'THE', 'SNACKING', 'BOISSONS FRAICHES', 'CHOCOLATS VAN HOUTEN'
                )
                -- Miroirs des inclusions CHARGEMENT ci-dessous : les volumes
                -- comptés via chargement ne doivent pas l'être via télémétrie.
                and not (
                    device_brand in ('NESTLE', 'ANIMO')
                    and (
                        device_economic_model not in (
                            'Participatif valeurs', 'Participatif unités', 'Payant'
                        )
                        or device_economic_model is null
                    )
                    and product_type = 'THE'
                )
                and not (
                    device_brand = 'NESPRESSO'
                    and (
                        device_economic_model not in (
                            'Participatif valeurs', 'Participatif unités', 'Payant'
                        )
                        or device_economic_model is null
                    )
                    and product_type in ('THE', 'CAFE CAPS')
                )
                and not (
                    device_brand in ('NESPRESSO', 'NESTLE', 'ANIMO')
                    and product_type = 'CHOCOLATS VAN HOUTEN'
                )
            )
            -- CHARGEMENT (machines gratuites + chocolats + accessoires)
            or (
                data_source = 'CHARGEMENT'
                and (
                    (
                        device_brand = 'NESPRESSO'
                        and (
                            device_economic_model not in (
                                'Participatif valeurs', 'Participatif unités', 'Payant'
                            )
                            or device_economic_model is null
                        )
                        and product_type in ('THE', 'CAFE CAPS')
                    )
                    or (
                        device_brand in ('NESTLE', 'ANIMO')
                        and (
                            device_economic_model not in (
                                'Participatif valeurs', 'Participatif unités', 'Payant'
                            )
                            or device_economic_model is null
                        )
                        and product_type = 'THE'
                    )
                    or (
                        device_brand in ('NESPRESSO', 'NESTLE', 'ANIMO')
                        and product_type = 'CHOCOLATS VAN HOUTEN'
                    )
                    or (product_type = 'ACCESSOIRES')
                )
            )
        )
),

combined_and_filtered_data as (
    select * from override_data
    union all
    select * from general_data
    union all
    -- LIVRAISON (jamais arbitrée)
    select * from livraison_data
)

select
    -- Identifiants
    company_id,
    device_id,
    location_id,
    product_id,

    -- Company
    company_code,
    company_name,

    -- Localisation
    location,

    -- Machine
    device_serial_number,
    device_name,
    device_brand,
    device_economic_model,

    -- Produit
    product_name,
    case
        when product_brand = 'BARRYCALLEBAUT' then 'VAN HOUTEN'
        else product_brand
    end as product_brand,
    product_family,
    product_group,
    product_type,

    -- Contexte
    consumption_date,
    data_source,

    -- Mesure
    quantity,

    -- Métadonnées d'exécution
    current_timestamp() as dbt_updated_at,
    '2cb31ec4-8ddf-4b38-841e-f02ea4b47c29' as dbt_invocation_id  -- noqa: TMP

from combined_and_filtered_data
    );
  