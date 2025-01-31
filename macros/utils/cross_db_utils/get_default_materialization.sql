{% macro get_default_materialization(type='table') %}
  {% do return(adapter.dispatch("get_default_materialization", "elementary")(type)) %}
{% endmacro %}

{% macro default__get_default_materialization(type='table') %}
  {% do return(type) %}
{% endmacro %}


{% macro clickhouse__get_default_materialization(type='table') %}
  {% set materialization_prefix = '' %}
  {% set distributed_type = type|trim|lower in ('table', 'incremental') %}
  {% if adapter.get_clickhouse_cluster_name() is not none and distributed_type %}
    {% set materialization_prefix = 'distributed_' %}
  {% endif %}
  {% set materialization = materialization_prefix ~ 'table' %}
  {% if is_incremental %}
    {% set materialization = materialization_prefix ~ 'incremental' %}
  {% endif %}
  {% do return(materialization) %}
{% endmacro %}
