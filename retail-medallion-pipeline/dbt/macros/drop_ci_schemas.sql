{# Clean up the per-PR schemas created by slim CI (dev target only). #}
{% macro drop_ci_schemas() %}
    {% if target.name == 'prod' %}
        {{ exceptions.raise_compiler_error("drop_ci_schemas must never run against prod") }}
    {% endif %}
    {% for layer in ['', '_silver', '_gold', '_audit'] %}
        {% set schema_name = target.schema ~ layer %}
        {% do run_query("drop schema if exists " ~ target.database ~ "." ~ schema_name ~ " cascade") %}
        {{ log("dropped " ~ schema_name, info=True) }}
    {% endfor %}
{% endmacro %}
