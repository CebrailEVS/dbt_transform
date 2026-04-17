
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_yuman__technicians`
      
    
    

    
    OPTIONS(
      description="""Dimension techniciens Yuman. Inclut les techniciens purs et les managers intervenant \u00e9galement comme technicien (is_manager_as_technician = true). Enrichie avec le nom du manager direct et le nom du d\u00e9p\u00f4t (storehouse) rattach\u00e9 au technicien via la jointure storehouses_id = user_id.\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users`
),

technicians as (
    select *
    from source
    where
        user_type = 'technician'
        or (user_type = 'manager' and is_manager_as_technician = true)
),

storehouses as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_yuman__storehouses`
),

final as (
    select
        -- ids
        t.user_id,
        t.nomad_id,
        t.manager_id,

        -- attributes
        t.user_name,
        t.user_email,
        t.user_phone,
        t.user_type,
        t.user_secteur,
        m.user_name as manager_name,
        s.storehouses_name,

        -- flags
        t.is_active,
        t.is_manager_as_technician,

        -- metadata
        t.created_at,
        t.updated_at
    from technicians as t
    left join source as m
        on t.manager_id = m.user_id
    left join storehouses as s
        on t.user_id = s.storehouses_id
)

select * from final
    );
  