{{
  config(
    materialized=elementary.get_default_materialization(type='incremental'),
    transient=False,
    post_hook='{{ elementary.upload_dbt_tests() }}',
    unique_key='unique_id',
    on_schema_change='sync_all_columns',
    full_refresh=elementary.get_config_var('elementary_full_refresh'),
    table_type=elementary.get_default_table_type(),
    incremental_strategy=elementary.get_default_incremental_strategy()
  )
}}

{{ elementary.get_dbt_tests_empty_table_query() }}
