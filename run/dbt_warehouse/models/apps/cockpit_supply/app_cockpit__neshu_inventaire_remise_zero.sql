
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_inventaire_remise_zero`
      
    
    cluster by entity_code, product_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Articles remis \u00e0 0 par un comptage d'inventaire Distrilog, dans un d\u00e9p\u00f4t ou un v\u00e9hicule Neshu. Distrilog n'exporte pas les stocks nuls : l'article dispara\u00eet de la photo au lieu d'y porter sa nouvelle date d'inventaire, et app_cockpit__neshu_inventaire_transition ne le voit pas. Destin\u00e9 au contr\u00f4le \u00ab Ajustements d'inventaire \u00bb de la cl\u00f4ture Neshu de Cockpit Supply.\n[COMMENT CONSTRUITE] T\u00e2ches \u00c9CART INVENTAIRE FAIT ou VALIDE (int_oracle_neshu__ecart_inventaire_tasks) plac\u00e9es entre deux photos cons\u00e9cutives de int_oracle_neshu__stock_theorique ; on garde l'article pr\u00e9sent dans la photo d'avant et absent de celle d'apr\u00e8s. Un m\u00eame comptage saisi en plusieurs versions : la plus r\u00e9cente (date du comptage, puis de cr\u00e9ation), sans additionner les versions. V\u00e9hicules marqu\u00e9s inactifs exclus, comme dans l'app.\n[GRAIN] 1 ligne par (entity_code, product_code, snapshot_date) : un article remis \u00e0 0 entre la photo pr\u00e9c\u00e9dente et la photo snapshot_date.\n[NOTES] qte_ecart = ajustement enregistr\u00e9 par Distrilog au moment du comptage ; stock_veille = stock de la photo d'avant, qui inclut les mouvements de la journ\u00e9e. Un comptage dont l'effet sur le stock arrive apr\u00e8s la photo suivante n'est pas rattach\u00e9. R\u00e9serv\u00e9 \u00e0 l'application, pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Distrilog n'exporte pas les stocks nuls : un article remis à 0 par un comptage disparaît de la photo au lieu
-- d'y porter sa nouvelle date d'inventaire, et app_cockpit__neshu_inventaire_transition ne le voit pas. On le
-- retrouve par la tâche ÉCART INVENTAIRE passée entre deux photos, sur un article présent avant et absent après.
with stock as (
    select
        snapshot_date,
        date_system,
        entity_code,
        product_code,
        date_inventaire,
        stock_at_date,
        dpa,
        is_vehicle_active
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__stock_theorique`
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

-- Un même comptage peut porter plusieurs tâches FAIT (versions saisies à quelques jours d'écart) : on garde
-- la plus récente par article et par intervalle, sans additionner les versions.
comptages as (
    select
        e.source_code as entity_code,
        e.product_code,
        e.task_id,
        e.task_product_id,
        e.task_status_code,
        e.task_start_date,
        e.quantity,
        i.snapshot_date_avant,
        i.snapshot_date_apres,
        i.photo_apres_at,
        count(*) over par_intervalle as nb_lignes_ecart
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__ecart_inventaire_tasks` as e
    inner join intervalles as i
        on
            e.task_start_date >= i.photo_avant_at
            and e.task_start_date < i.photo_apres_at
    where e.task_status_code in ('FAIT', 'VALIDE')
    qualify row_number() over (
        partition by e.source_code, e.product_code, i.snapshot_date_apres
        order by e.task_start_date desc, e.created_at desc, e.task_product_id desc
    ) = 1
    window par_intervalle as (partition by e.source_code, e.product_code, i.snapshot_date_apres)
)

select
    c.photo_apres_at as date_system,
    c.snapshot_date_apres as snapshot_date,
    c.entity_code,
    c.product_code,
    c.task_start_date as date_inventaire,
    avant.date_inventaire as date_inventaire_precedente,
    avant.stock_at_date as stock_veille,
    c.quantity as qte_ecart,
    avant.dpa as dpa_veille,
    c.task_id,
    c.task_product_id,
    c.task_status_code,
    c.nb_lignes_ecart
from comptages as c
inner join stock as avant
    on
        c.snapshot_date_avant = avant.snapshot_date
        and c.entity_code = avant.entity_code
        and c.product_code = avant.product_code
left join stock as apres
    on
        c.snapshot_date_apres = apres.snapshot_date
        and c.entity_code = apres.entity_code
        and c.product_code = apres.product_code
where
    apres.product_code is null
    -- Même périmètre que l'app (_filter_vehicule_actif) : on écarte les véhicules marqués inactifs.
    and not (
        (
            regexp_contains(c.entity_code, r'^V\d+')
            or c.entity_code in ('ARKEMA', 'STRASBOURG', 'RATP')
        )
        and coalesce(avant.is_vehicle_active = false, false)
    )
    );
  