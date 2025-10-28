{% macro get_last_loaded_ts(schema_table_name, column_name) %}

    {% set select_sql %}

        select max({{column_name}}) as last_loaded_ts 
        from {{schema_table_name}}

    {% endset %}

    {% set results = run_query(select_sql) %}

    {% if results and results.columns[0].values()[0] %}
        {% set last_loaded_ts = results.columns[0].values()[0] %}
    {% else %}
        {% set last_loaded_ts = '1900-01-01' %}
    {% endif %}

    {{ log("Last loaded timestamp: " ~ last_loaded_ts, info=True) }}

    {{return(last_loaded_ts)}}

{% endmacro %}