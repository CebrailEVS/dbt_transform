





with validation_errors as (

    select
        _export_yuman, _ligne
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_web__mouvements_stock`
    group by _export_yuman, _ligne
    having count(*) > 1

)

select *
from validation_errors


