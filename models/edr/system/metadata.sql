{{ config(materialized=elementary.get_default_materialization(type='table')) }}

SELECT
    '{{ elementary.get_elementary_package_version() }}' as dbt_pkg_version
