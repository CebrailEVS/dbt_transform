

    
    


    
    




with window_functions as (

    select
        
        key_ref_inter as partition_by_col,
        
        valid_from as lower_bound,
        valid_to as upper_bound,

        lead(valid_from) over (
            partition by key_ref_inter
            order by valid_from, valid_to
        ) as next_lower_bound,

        row_number() over (
            partition by key_ref_inter
            order by valid_from desc, valid_to desc
        ) = 1 as is_last_record

    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation`

),

calc as (
    -- We want to return records where one of our assumptions fails, so we'll use
    -- the `not` function with `and` statements so we can write our assumptions more cleanly
    select
        *,

        -- For each record: lower_bound should be < upper_bound.
        -- Coalesce it to return an error on the null case (implicit assumption
        -- these columns are not_null)
        coalesce(
            lower_bound < upper_bound,
            false
        ) as lower_bound_less_than_upper_bound,

        -- For each record: upper_bound <= the next lower_bound.
        -- Coalesce it to handle null cases for the last record.
        coalesce(
            upper_bound <= next_lower_bound,
            is_last_record,
            false
        ) as upper_bound_less_than_or_equal_to_next_lower_bound

    from window_functions

),

validation_errors as (

    select
        *
    from calc

    where not(
        -- THE FOLLOWING SHOULD BE TRUE --
        lower_bound_less_than_upper_bound
        and upper_bound_less_than_or_equal_to_next_lower_bound
    )
)

select * from validation_errors
