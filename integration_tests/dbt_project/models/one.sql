{{
    config(
        materialized=elementary.get_default_materialization(type='table'),
        tags=var('one_tags', []),
        meta={'owner': var('one_owner', 'egk')}
    )
}}

SELECT 1 AS one
