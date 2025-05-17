{% macro count_grouped_records(source_table, group_cols, count_col, alias='Num_Records') %}
(
    SELECT
        {% for col in group_cols %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        COUNT({{ count_col }}) AS {{ alias }}
    FROM {{ ref(source_table) }}
    GROUP BY
        {% for col in group_cols %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
)
{% endmacro %}