
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  






with recency as (

    select 

      
      
        max(loaded_at) as most_recent

    from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__passages_appro`

    

)

select

    
    most_recent,
    cast(

        datetime_add(
            cast( current_timestamp() as datetime),
        interval -24 hour
        )

 as timestamp) as threshold

from recency
where most_recent < cast(

        datetime_add(
            cast( current_timestamp() as datetime),
        interval -24 hour
        )

 as timestamp)


  
  
      
    ) dbt_internal_test