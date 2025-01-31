{{ config(materialized=elementary.get_default_materialization(type='non_dbt')) }}
    SELECT 1
-- depends_on: {{ ref('one') }}