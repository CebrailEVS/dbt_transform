

-- PAS D'INCRÉMENTAL : chaque run réécrit les 7 dernières journées avec un nouvel `_extracted_at`,
-- un ajout filtré sur `extracted_at` les doublerait.

with source as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`web_yuman_evs_mouvements_stock`

),

cleaned as (

    select
        -- Les noms bruts sont mutilés par le normaliseur dlt (`qtx` = « Qté », `r_ference` =
        -- « Réference ») : renommés d'après l'en-tête du CSV.

        -- Heure de Paris à la minute, sans fuseau dans le fichier.
        timestamp(datetime(parse_datetime('%d/%m/%Y - %H:%M', date_de_l_op_ration)), 'Europe/Paris') as operation_at,
        journee,

        trim(r_ference) as reference,
        trim(article) as article,
        nullif(trim(cat_gorie), '') as categorie,
        nullif(trim(sous_cat_gorie), '') as sous_categorie,
        nullif(trim(equipement), '') as equipement,

        -- Virgule décimale. Toujours positive : le sens du mouvement se lit dans les stocks.
        cast(replace(qtx, ',', '.') as float64) as quantite,
        nullif(trim(num_ro_de_s_rie), '') as numero_de_serie,
        cast(replace(nullif(trim(prix_d_achat_unitaire), ''), ',', '.') as numeric) as prix_d_achat_unitaire,
        nullif(trim(devise_d_achat), '') as devise_d_achat,
        nullif(trim(site), '') as site,
        nullif(trim(adresse), '') as adresse,

        -- Les libellés de stock arrivent avec des espaces de tête.
        nullif(trim(stock_de_d_part), '') as stock_de_depart,
        cast(replace(nullif(trim(qt_stock_de_d_part), ''), ',', '.') as float64) as quantite_stock_de_depart,
        nullif(trim(stock_de_destination), '') as stock_de_destination,
        cast(
            replace(nullif(trim(qt_stock_de_destination), ''), ',', '.') as float64
        ) as quantite_stock_de_destination,

        nullif(trim(utilisateur), '') as utilisateur,
        nullif(trim(observation), '') as observation,

        _export_yuman,
        -- ORDRE : l'instant est à la minute, `_ligne` (rang dans l'export) départage les mouvements
        -- d'une même minute. Ne se compare qu'à l'intérieur d'une journée.
        cast(_ligne as int64) as _ligne,
        timestamp(_extracted_at) as extracted_at,

        -- CLÉ DE LIGNE : pas de clé naturelle, des mouvements identiques à la minute sont réels.
        -- `_dlt_id` est régénéré à chaque rechargement : jamais une clé persistante en aval.
        _dlt_id

    from source

)

select *
from cleaned