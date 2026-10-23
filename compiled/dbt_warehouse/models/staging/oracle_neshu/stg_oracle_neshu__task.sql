

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_task`
),

cleaned_data as (
    select
        -- IDs (clés primaires et étrangères)
        cast(idtask as int64) as idtask,
        cast(task_idtask as int64) as task_idtask,
        cast(idtask_type as int64) as idtask_type,
        cast(idtask_status as int64) as idtask_status,
        cast(iddevice as int64) as iddevice,
        cast(idcompany_peer as int64) as idcompany_peer,
        cast(idlocation as int64) as idlocation,
        cast(idcontact as int64) as idcontact,
        cast(idproduct_source as int64) as idproduct_source,
        cast(idproduct_destination as int64) as idproduct_destination,

        -- Colonnes texte et types
        cast(document_number as string) as document_number, -- n° de pièce affiché dans l'ERP (ex. ICRSOLLEAU0)
        type_product_source,
        type_product_destination,
        -- Cast explicite : le modèle est incrémental, un MERGE ne tolère pas un changement de type du raw.
        cast(comments_self as string) as comments_self,
        cast(comments_peer as string) as comments_peer,
        -- Zone XML libre de la tâche, exposée brute : le XML est laissé brut ; il est parsé en aval
        -- quand un modèle en a besoin. Porte notamment /ZONE/COUTRM, l'enveloppe mensuelle de charges
        -- sociales (type 242).
        -- Renseignée sur une petite minorité de tâches.
        xml,

        -- Colonne numérique
        -- Cast explicite : le modèle ne dépend pas du type du raw.
        cast(spantime as float64) as spantime, -- durée de la tâche en minute
        -- status record (1 = validé, 0 ou -1 possiblement inactif/désactivé)
        cast(code_status_record as string) as code_status_record,

        -- Date de la tache
        timestamp(datetime(real_start_date), 'Europe/Paris') as real_start_date,
        timestamp(datetime(real_end_date), 'Europe/Paris') as real_end_date,
        timestamp(datetime(planed_start_date), 'Europe/Paris') as planed_start_date,
        timestamp(datetime(planed_end_date), 'Europe/Paris') as planed_end_date,

        -- Timestamps harmonisés
        timestamp(datetime(creation_date), 'Europe/Paris') as created_at,
        -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
        timestamp(datetime(coalesce(modification_date, creation_date)), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data

    where
        (
            updated_at > (
                select max(t.updated_at)
                from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
            )
            or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
        )
