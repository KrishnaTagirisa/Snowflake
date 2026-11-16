{#
  prod  -> use the layer schema as-is:            SILVER, GOLD
  dev   -> isolate each developer's build:        DBT_KRISHNA_SILVER, DBT_KRISHNA_GOLD
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim | upper }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
