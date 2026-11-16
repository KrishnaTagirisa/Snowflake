-- =============================================================================
-- assert_no_future_or_implausible_dob.sql
-- Singular test: fails if any staged customer has a date_of_birth in the
-- future, or implies an age over 120 -- both indicate upstream data errors
-- that generic schema tests (not_null/unique) wouldn't catch.
-- A passing test returns zero rows.
-- =============================================================================

select
    customer_id,
    date_of_birth
from {{ ref('stg_customers') }}
where date_of_birth > current_date()
   or datediff(year, date_of_birth, current_date()) > 120
