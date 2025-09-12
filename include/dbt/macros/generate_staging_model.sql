{% macro generate_staging_model(source_name, table_name, column_casts, transformations = {}) %}
    SELECT
    {% for col_name, col_type in column_casts.items() %}
        {%- if col_name in transformations %}
            {{ transformations[col_name] }} AS {{ col_name }}
        {%- else %}
            {{ col_name }}::{{ col_type }} AS {{ col_name }}
        {%- endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    FROM {{ source(source_name, table_name) }}
{% endmacro %}