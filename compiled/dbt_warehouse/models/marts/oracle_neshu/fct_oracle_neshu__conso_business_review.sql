

with telemetry_data as (
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
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device` as d
        on t.device_id = d.device_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` as p
        on t.product_id = p.product_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company` as c
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

        -- Quantité (ajustée selon le produit)
        sum(
            case
                when p.product_name like '%GOBELET%RAME 50%' then l.load_quantity * 50
                when p.product_name like '%GOBELET%RAME DE 30%' then l.load_quantity * 30
                when p.product_name like '%GOBELET%RAME 35%' then l.load_quantity * 35
                when p.product_name like '%MELANG%BTE 200%' then l.load_quantity * 200
                when p.product_name like '%MELANGEUR%BTE 200%' then l.load_quantity * 200
                when p.product_name like '%MELANGEUR%BTE 100%' then l.load_quantity * 100
                when p.product_name like '%BEGHIN SAY 300%' then l.load_quantity * 300
                when p.product_name like '%CARTON DE 500%' then l.load_quantity * 500
                when p.product_name like '%DISTRIBUTEUR 300 SUCRES%'
                    then l.load_quantity * 300
                when p.product_name like '%SUCRE BATONNET 100%' then l.load_quantity * 100
                when p.product_name like '%SUCRE BTE 300%' then l.load_quantity * 300
                when p.product_name like '%NESPRESSO MELANGEURS EN BAMBOU INDI%'
                    then l.load_quantity * 100
                else l.load_quantity
            end
        ) as quantity

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks` as l
    inner join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device` as d
        on l.device_id = d.device_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` as p
        on l.product_id = p.product_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company` as c
        on l.company_id = c.company_id
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

        -- Quantité (ajustée selon le produit)
        sum(
            case
                when p.product_name like '%GOBELET%RAME 50%' then lt.quantity * 50
                when p.product_name like '%GOBELET%RAME DE 30%' then lt.quantity * 30
                when p.product_name like '%GOBELET%RAME 35%' then lt.quantity * 35
                when p.product_name like '%MELANG%BTE 200%' then lt.quantity * 200
                when p.product_name like '%MELANGEUR%BTE 200%' then lt.quantity * 200
                when p.product_name like '%MELANGEUR%BTE 100%' then lt.quantity * 100
                when p.product_name like '%BEGHIN SAY 300%' then lt.quantity * 300
                when p.product_name like '%CARTON DE 500%' then lt.quantity * 500
                when p.product_name like '%DISTRIBUTEUR 300 SUCRES%'
                    then lt.quantity * 300
                when p.product_name like '%SUCRE BATONNET 100%' then lt.quantity * 100
                when p.product_name like '%SUCRE BTE 300%' then lt.quantity * 300
                when p.product_name like '%NESPRESSO MELANGEURS EN BAMBOU INDI%'
                    then lt.quantity * 100
                else lt.quantity
            end
        ) as quantity

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__livraison_tasks` as lt
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` as p
        on lt.product_id = p.product_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company` as c
        on lt.company_id = c.company_id
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

-- Version optimisée : remplace combined_data + donnees_filtrees
combined_and_filtered_data as (

    -- Cas particulier : machine AS00446 chez CN1046
    select *
    from chargement_data
    where
        device_serial_number = 'AS00446'
        and company_code = 'CN1046'
        and consumption_date < '2025-08-28'
        and product_type in ('THE', 'CAFE CAPS', 'CHOCOLATS VAN HOUTEN')

    union all

    select *
    from telemetry_data
    where
        device_serial_number = 'AS00446'
        and company_code = 'CN1046'
        and consumption_date >= '2025-08-28'

    union all

    -- TELEMETRIE avec filtres
    select *
    from telemetry_data
    where
        product_type in (
            'BOISSONS GOURMANDES', 'CAFE CAPS', 'CAFENOIR', 'INDEFINI',
            'THE', 'SNACKING', 'BOISSONS FRAICHES', 'CHOCOLATS VAN HOUTEN'
        )
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
        and not (company_code = 'CN1071' and product_type = 'THE')
        and not (
            device_serial_number = 'AS00446' and company_code = 'CN1046'
        )

    union all

    -- CHARGEMENT avec tous les filtres consolidés
    select *
    from chargement_data
    where (
        (
            device_brand = 'NESPRESSO'
            and (
                device_economic_model not in (
                    'Participatif valeurs', 'Participatif unités', 'Payant'
                )
                or device_economic_model is null
            )
            and product_type in ('THE', 'CAFE CAPS')
            and (
                company_code <> 'CN1070'
                or consumption_date >= '2025-03-01'
            )
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
        or (company_code = 'CN1071' and product_type = 'THE')
        and not (
            device_serial_number = 'AS00446' and company_code = 'CN1046'
        )
    )

    union all

    -- LIVRAISON (tous les enregistrements)
    select *
    from livraison_data
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
    '8682d878-0f24-4d24-b798-7e44f50d561c' as dbt_invocation_id  -- noqa: TMP

from combined_and_filtered_data