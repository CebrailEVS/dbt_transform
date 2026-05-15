{% macro business_minutes_between(start_ts, end_ts) %}
    (
        select ifnull(
            sum(
                greatest(
                    timestamp_diff(
                        least(
                            timestamp(datetime(d, time(17, 30, 0)), 'Europe/Paris'),
                            {{ end_ts }}
                        ),
                        greatest(
                            timestamp(datetime(d, time(9, 0, 0)), 'Europe/Paris'),
                            {{ start_ts }}
                        ),
                        minute
                    ),
                    0
                )
            ),
            0
        )
        from unnest(
            generate_date_array(
                date({{ start_ts }}, 'Europe/Paris'),
                date({{ end_ts }}, 'Europe/Paris')
            )
        ) as d
        where extract(dayofweek from d) not in (1, 7)
            and d not in (
                select cast(date_ferie as date)
                from {{ ref('ref_general__feries_metropole') }}
            )
    )
{% endmacro %}
