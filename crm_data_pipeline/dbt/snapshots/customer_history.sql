{% snapshot customer_history %}
{{ config(target_schema='SILVER', unique_key='customer_id', strategy='timestamp',
          updated_at='updated_at', invalidate_hard_deletes=True) }}
select customer_id,email,first_name,last_name,phone,updated_at
from {{ ref('silver_customers_current') }}
{% endsnapshot %}
