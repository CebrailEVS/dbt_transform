

-- Commandes internes des approvisionneurs Cafés du Phare (132) et horodatage de leurs étapes, pour les
-- délais de traitement des commandes (ISO) de Cockpit Supply. Les délais et leur conformité sont calculés
-- par l'app. Le mouvement de stock (161) est créé AVANT la préparation, au bureau ; la préparation est
-- datée par le terminal mobile : début et fin réels du mouvement, seulement quand ils diffèrent (une
-- validation au bureau les laisse égaux). Les commandes clients ne sont pas saisies dans Distrilog.
with taches as (
    select
        idtask,
        task_idtask,
        idtask_type,
        idtask_status,
        type_product_source,
        idproduct_source,
        type_product_destination,
        real_start_date,
        real_end_date,
        created_at
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task`
    where
        idtask_type in (132, 161)
        and code_status_record = '1'
),

mouvements as (
    select
        task_idtask,
        min(created_at) as mouvement_cree_le,
        min(if(real_end_date > real_start_date, real_start_date, null)) as preparation_debut_le,
        max(if(real_end_date > real_start_date, real_end_date, null)) as preparation_fin_le,
        logical_or(idtask_status in (1, 4)) as is_mouvement_termine -- FAIT, VALIDE
    from taches
    where
        idtask_type = 161
        and task_idtask is not null
    group by task_idtask
),

enrichies as (
    select
        c.idtask,
        c.created_at,
        ts.code as statut_code,
        if(c.type_product_source = 'COMPANY', co.code, null) as depot_code,
        c.type_product_source = 'COMPANY' and c.type_product_destination = 'RESOURCES' as is_depot_vers_vehicule,
        m.mouvement_cree_le,
        m.preparation_debut_le,
        m.preparation_fin_le,
        coalesce(m.is_mouvement_termine, false) as is_mouvement_termine
    from taches as c
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_status` as ts
        on c.idtask_status = ts.idtask_status
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as co
        on c.idproduct_source = co.idcompany
    left join mouvements as m
        on c.task_idtask = m.task_idtask
    where c.idtask_type = 132
)

select
    idtask as commande_id,
    'INTERNE' as type_commande,
    depot_code,
    'METROPOLE' as calendrier,
    statut_code,
    timestamp(datetime(created_at, 'Europe/Paris')) as cree_le,
    timestamp(datetime(mouvement_cree_le, 'Europe/Paris')) as mouvement_cree_le,
    timestamp(datetime(preparation_debut_le, 'Europe/Paris')) as preparation_debut_le,
    timestamp(datetime(preparation_fin_le, 'Europe/Paris')) as preparation_fin_le,
    case
        when statut_code = 'ANNULE' then 'ANNULEE'
        -- Transferts vers la boutique et entre dépôts : mouvements internes, hors périmètre
        when not is_depot_vers_vehicule then 'HORS_CHARGEMENT_VEHICULE'
        -- Mouvement terminé sans passer par le terminal mobile : préparation non datée
        when is_mouvement_termine and preparation_fin_le is null then 'PREPARATION_SANS_MOBILE'
    end as motif_exclusion
from enrichies