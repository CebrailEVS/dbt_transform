

-- Livraisons internes Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app, tâches validées
-- seulement (FAIT, VALIDE), comme l'app. S'y ajoutent les tâches de fabrication saisies pour un simple transfert
-- (is_transfert) : elles déplacent du stock sans torréfaction, l'app les compte donc comme des transferts.
with mouvements as (
    select
        task_product_id,
        task_id,
        task_start_date,
        task_status_code,
        source_code,
        destination_code,
        product_code,
        quantity,
        valuation,
        'LIVRAISON_INTERNE' as type_tache
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__livraison_interne_tasks`
    where task_status_code in ('FAIT', 'VALIDE')

    union all

    select
        task_product_id,
        task_id,
        task_start_date,
        task_status_code,
        source_code,
        destination_code,
        product_code,
        quantity,
        valuation,
        'SORTIE_FABRICATION' as type_tache
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__sortie_fabrication_tasks`
    where task_status_code in ('FAIT', 'VALIDE') and is_transfert

    union all

    select
        task_product_id,
        task_id,
        task_start_date,
        task_status_code,
        source_code,
        destination_code,
        product_code,
        quantity,
        valuation,
        'ENTREE_FABRICATION' as type_tache
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__entree_fabrication_tasks`
    where task_status_code in ('FAIT', 'VALIDE') and is_transfert
)

select
    task_product_id,
    task_id,
    timestamp(datetime(task_start_date, 'Europe/Paris')) as task_start_date,
    task_status_code,
    source_code,
    destination_code,
    product_code,
    quantity,
    valuation,
    type_tache
from mouvements