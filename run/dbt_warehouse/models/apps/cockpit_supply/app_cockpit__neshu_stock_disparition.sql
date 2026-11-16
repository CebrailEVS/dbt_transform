
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_stock_disparition`
      
    
    cluster by entity_code, product_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Articles sortis de la photo de stock d'un d\u00e9p\u00f4t ou d'un v\u00e9hicule Neshu : pr\u00e9sents dans une photo, absents de la suivante, c'est-\u00e0-dire tomb\u00e9s \u00e0 0 (Distrilog n'exporte pas les stocks nuls). Chaque disparition porte la t\u00e2che d'\u00e9cart d'inventaire qui la prouve, s'il y en a une. Destin\u00e9 \u00e0 la cl\u00f4ture Neshu de Cockpit Supply : ajustements d'inventaire (disparitions prouv\u00e9es) et contr\u00f4le des disparitions sans t\u00e2che, \u00e0 rapprocher des mouvements du flux.\n[COMMENT CONSTRUITE] Photos cons\u00e9cutives de int_oracle_neshu__stock_theorique ; une s\u00e9rie (entity_code, product_code) dispara\u00eet quand sa ligne suivante n'est pas dans la photo suivante. T\u00e2che rattach\u00e9e (int_oracle_neshu__ecart_inventaire_tasks, FAIT ou VALIDE) : comptage pass\u00e9 avant la photo sans l'article, post\u00e9rieur \u00e0 la date d'inventaire que la ligne portait encore la veille, dans les 15 jours, car l'effet d'un comptage peut attendre sa validation ; hors de l'intervalle des deux photos, l'\u00e9cart doit \u00eatre de signe oppos\u00e9 au stock de la veille. Versions d'un m\u00eame comptage : la plus r\u00e9cente, sans les additionner ; une ligne de t\u00e2che ne prouve qu'une disparition. V\u00e9hicules marqu\u00e9s inactifs exclus, comme dans l'app.\n[GRAIN] 1 ligne par (entity_code, product_code, snapshot_date) : un article pr\u00e9sent dans la photo pr\u00e9c\u00e9dente et absent de la photo snapshot_date.\n[NOTES] stock_veille = 0 : ligne export\u00e9e \u00e0 0 la veille, pas de changement de stock. date_reapparition et stock_reapparition disent si l'article revient ensuite, par exemple une absence d'un jour. R\u00e9serv\u00e9 \u00e0 l'application, pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Distrilog n'exporte pas les stocks nuls : un article dont le stock tombe à 0 disparaît de la photo. Une ligne par
-- disparition (présent dans une photo, absent de la suivante), avec la tâche ÉCART INVENTAIRE qui la prouve s'il y en a
-- une ; sans tâche, l'article est sorti par des mouvements ou sans trace.
with stock as (
    select
        snapshot_date,
        date_system,
        entity_code,
        product_code,
        date_inventaire,
        stock_at_date,
        dpa,
        is_vehicle_active,
        lead(snapshot_date) over par_serie as snapshot_date_suivante,
        lead(stock_at_date) over par_serie as stock_suivant
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__stock_theorique`
    window par_serie as (partition by entity_code, product_code order by snapshot_date)
),

photos as (
    select
        snapshot_date,
        max(date_system) as photo_at
    from stock
    group by snapshot_date
),

intervalles as (
    select
        snapshot_date as snapshot_date_avant,
        photo_at as photo_avant_at,
        lead(snapshot_date) over (order by snapshot_date) as snapshot_date_apres,
        lead(photo_at) over (order by snapshot_date) as photo_apres_at
    from photos
),

disparitions as (
    select
        i.photo_avant_at,
        i.snapshot_date_apres,
        i.photo_apres_at,
        s.snapshot_date as snapshot_date_veille,
        s.entity_code,
        s.product_code,
        s.date_inventaire as date_inventaire_precedente,
        s.stock_at_date as stock_veille,
        s.dpa as dpa_veille,
        s.snapshot_date_suivante as date_reapparition,
        s.stock_suivant as stock_reapparition
    from stock as s
    inner join intervalles as i on s.snapshot_date = i.snapshot_date_avant
    where
        i.snapshot_date_apres is not null
        and (s.snapshot_date_suivante is null or s.snapshot_date_suivante > i.snapshot_date_apres)
        -- Même périmètre que l'app (_filter_vehicule_actif) : on écarte les véhicules marqués inactifs.
        and not (
            (
                regexp_contains(s.entity_code, r'^V\d+')
                or s.entity_code in ('ARKEMA', 'STRASBOURG', 'RATP')
            )
            and coalesce(s.is_vehicle_active = false, false)
        )
),

-- Preuve : comptage FAIT ou VALIDE pas encore appliqué la veille (postérieur à la date d'inventaire de la ligne). Son
-- effet peut attendre la validation, quelques jours : fenêtre de 15 jours, avec hors de l'intervalle des deux photos
-- un écart de signe opposé au stock de la veille. Versions d'un comptage : la plus récente, sans les additionner.
ecarts as (
    select
        d.entity_code,
        d.product_code,
        d.snapshot_date_apres,
        e.task_id,
        e.task_product_id,
        e.task_status_code,
        e.task_start_date,
        e.quantity,
        count(*) over par_disparition as nb_lignes_ecart
    from disparitions as d
    inner join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__ecart_inventaire_tasks` as e
        on
            d.entity_code = e.source_code
            and d.product_code = e.product_code
            and d.photo_apres_at > e.task_start_date
            and timestamp_sub(d.photo_apres_at, interval 15 day) <= e.task_start_date
            and coalesce(d.date_inventaire_precedente, timestamp('1900-01-01')) < e.task_start_date
            and (
                d.photo_avant_at <= e.task_start_date
                or -sign(d.stock_veille) = sign(e.quantity)
            )
    where e.task_status_code in ('FAIT', 'VALIDE')
    -- Une ligne de tâche ne prouve qu'une disparition : la première après le comptage.
    qualify
        row_number() over (
            partition by d.entity_code, d.product_code, d.snapshot_date_apres
            order by e.task_start_date desc, e.created_at desc, e.task_product_id desc
        ) = 1
        and row_number() over (partition by e.task_product_id order by d.snapshot_date_apres) = 1
    window par_disparition as (partition by d.entity_code, d.product_code, d.snapshot_date_apres)
)

select
    d.photo_apres_at as date_system,
    d.snapshot_date_apres as snapshot_date,
    d.snapshot_date_veille,
    d.entity_code,
    d.product_code,
    d.date_inventaire_precedente,
    d.stock_veille,
    d.dpa_veille,
    d.date_reapparition,
    d.stock_reapparition,
    e.task_id is not null as is_remise_inventaire,
    e.task_start_date as date_inventaire,
    e.quantity as qte_ecart,
    e.task_id,
    e.task_product_id,
    e.task_status_code,
    e.nb_lignes_ecart
from disparitions as d
left join ecarts as e
    on
        d.entity_code = e.entity_code
        and d.product_code = e.product_code
        and d.snapshot_date_apres = e.snapshot_date_apres
    );
  