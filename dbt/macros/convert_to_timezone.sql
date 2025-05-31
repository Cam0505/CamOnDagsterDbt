{% macro convert_to_timezone(column_name, timezone='Australia/Melbourne') %}
    {{ column_name }} AT TIME ZONE 'UTC' AT TIME ZONE '{{ timezone }}'
{% endmacro %}