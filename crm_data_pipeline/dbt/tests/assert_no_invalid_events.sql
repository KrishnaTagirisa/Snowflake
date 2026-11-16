select * from {{ ref('stg_crm_events') }} where validation_error is not null
