{% test ratio_condition(model, condition, column_name=None, max_ratio=0.01, where=None) %}
    with base as (
        select *
        from {{ model }}
        {% if where %}
        where {{ where }}
        {% endif %}
    ),
    stats as (
        select
            count(*) filter (where ({{ condition }}))::float as bad_count,
            count(*)::float as total_count
        from base
    )
    select
        bad_count,
        total_count,
        bad_count / nullif(total_count, 0) as bad_ratio
    from stats
    where (bad_count / nullif(total_count, 0)) > {{ max_ratio }}
{% endtest %}
