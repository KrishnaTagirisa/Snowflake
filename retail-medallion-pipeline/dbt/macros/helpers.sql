{# ---------------------------------------------------------------------------
   incremental_filter: only pick up bronze rows newer than what this model has
   already processed, minus a look-back window for late-arriving data.
--------------------------------------------------------------------------- #}
{% macro incremental_filter(source_ts_col='_loaded_at', target_ts_col='_bronze_loaded_at') -%}
    {%- if is_incremental() -%}
    where {{ source_ts_col }} > (
        select dateadd(hour, -{{ var('lookback_hours') }},
                       coalesce(max({{ target_ts_col }}), '1900-01-01'::timestamp_ltz))
        from {{ this }}
    )
    {%- endif -%}
{%- endmacro %}


{# Standard silver audit columns #}
{% macro silver_audit_columns() -%}
    current_timestamp()::timestamp_ltz as _dbt_updated_at,
    '{{ invocation_id }}'              as _dbt_invocation_id
{%- endmacro %}


{# Clean a free-text string: trim, collapse blanks to NULL #}
{% macro clean_str(col) -%}
    nullif(trim({{ col }}), '')
{%- endmacro %}


{# Parse a source timestamp that may arrive in several formats #}
{% macro parse_ts(col) -%}
    coalesce(
        try_to_timestamp_ntz({{ col }}, 'YYYY-MM-DD HH24:MI:SS'),
        try_to_timestamp_ntz({{ col }}, 'YYYY-MM-DD"T"HH24:MI:SS'),
        try_to_timestamp_ntz({{ col }}, 'DD/MM/YYYY HH24:MI:SS'),
        try_to_timestamp_ntz({{ col }})
    )
{%- endmacro %}


{# Grant SELECT on the built relation (used as a post-hook on gold) #}
{% macro grant_select(role) -%}
    {%- if target.name == 'prod' -%}
        grant select on {{ this }} to role {{ role }}
    {%- else -%}
        select 1  {# no-op outside prod #}
    {%- endif -%}
{%- endmacro %}
