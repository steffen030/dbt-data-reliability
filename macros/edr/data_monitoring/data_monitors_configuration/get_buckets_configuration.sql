{# This macro can't be used without truncating to full buckets #}
{% macro get_min_bucket_start(days_back) %}
    {% do return((elementary.get_run_started_at() - modules.datetime.timedelta(days_back | int)).strftime("%Y-%m-%d 00:00:00")) %}
{% endmacro %}

{% macro get_trunc_min_bucket_start_expr(metric_properties, min_bucket_start) %}
    {%- set trunc_min_bucket_start_expr = elementary.edr_date_trunc(metric_properties.time_bucket.period, elementary.edr_cast_as_timestamp(elementary.edr_quote(min_bucket_start)))%}
    {{ return(trunc_min_bucket_start_expr) }}
{% endmacro %}

-- TODO: This needs to be truncated according to the latest full bucket
{% macro get_max_bucket_end() %}
    {% do return(elementary.run_started_at_as_string()) %}
{% endmacro %}

{# This macro can't be used without truncating to full buckets #}
{% macro get_backfill_bucket_start(backfill_days, metric_properties) %}
    {% do return((elementary.get_run_started_at() - modules.datetime.timedelta(backfill_days)).strftime("%Y-%m-%d 00:00:00")) %}
{% endmacro %}


--TODO: make sure buckets are full when table is not incremental
{% macro get_test_min_bucket_start(model_graph_node, backfill_days, days_back, monitors=none, column_name=none, metric_properties=none) %}

    {%- set min_bucket_start = elementary.get_min_bucket_start(days_back) %}
    {# We assume we should also cosider sources as incremental #}
    {% if not elementary.is_incremental_model(model_graph_node, source_included=true) %}
        {% do return(min_bucket_start) %}
    {% endif %}

    {%- set run_start_expr = elementary.edr_quote(elementary.get_run_started_at().strftime("%Y-%m-%d %H:00:00")) %}
    {%- set trunc_min_bucket_start_expr = elementary.get_trunc_min_bucket_start_expr(metric_properties, min_bucket_start) %}
    {%- set backfill_bucket_start = elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_backfill_bucket_start(backfill_days))) %}
    {%- set full_table_name = elementary.model_node_to_full_name(model_graph_node) %}
    {%- if monitors %}
        {%- set monitors_tuple = elementary.strings_list_to_tuple(monitors) %}
    {%- endif %}

    {%- set bucket_times_query %}
        with bucket_times as (
            select min(last_bucket_end) as last_max_bucket_end,
                   min(first_bucket_end) as last_min_bucket_end,
                   {{ trunc_min_bucket_start_expr }} as days_back_start,
                   {{ backfill_bucket_start }} as backfill_start,
                   {{ run_start_expr }} as run_started
            from {{ ref('monitors_runs') }}
            where upper(full_table_name) = upper('{{ full_table_name }}')
              and metric_properties = {{ elementary.dict_to_quoted_json(metric_properties) }}
            {%- if monitors %}
                and metric_name in {{ monitors_tuple }}
            {%- endif %}
            {%- if column_name %}
                and upper(column_name) = upper('{{ column_name }}')
            {%- endif %}
            ),
        full_buckets_calc as (
            select *,
                {# How many periods we need to reduce from last_max_bucket_end to backfill full time buckets #}
                case
                    when last_max_bucket_end is not null
                    then ceil({{ elementary.edr_datediff('last_max_bucket_end', 'backfill_start', metric_properties.time_bucket.period) }} / {{ metric_properties.time_bucket.count }}) * {{ metric_properties.time_bucket.count }}
                    else 0
                end as periods_to_backfill,
                {# How many periods we need to add to last run time to get only full time buckets #}
                case
                    when last_max_bucket_end is not null
                    then floor({{ elementary.edr_datediff('last_max_bucket_end', 'run_started', metric_properties.time_bucket.period) }} / {{ metric_properties.time_bucket.count }}) * {{ metric_properties.time_bucket.count }}
                    else floor({{ elementary.edr_datediff('days_back_start', 'run_started', metric_properties.time_bucket.period) }} / {{ metric_properties.time_bucket.count }}) * {{ metric_properties.time_bucket.count }}
                end as periods_until_max
            from bucket_times
        )
        select
            case
                {# This prevents gaps in buckets for the metric #}
                when last_max_bucket_end is null then days_back_start {# This is the first run of this metric #}
                when last_max_bucket_end < days_back_start then days_back_start {# The metric was not collected for a period longer than days_back #}
                when last_min_bucket_end > days_back_start then days_back_start {# The metric was collected recently, but for a period that is smaller than days_back #}
                when last_max_bucket_end < backfill_start then last_max_bucket_end {# The metric was not collected for a period longer than backfill_days #}
                else {{ elementary.edr_timeadd(metric_properties.time_bucket.period, 'periods_to_backfill', 'last_max_bucket_end') }} {# Reduce full time buckets from last_max_bucket_end to backfill #}
            end as min_bucket_start,
            case
                {# This makes sure we collect only full bucket #}
                when last_max_bucket_end is null
                then {{ elementary.edr_timeadd(metric_properties.time_bucket.period, 'periods_until_max', 'days_back_start') }} {# Add full buckets to days_back_start #}
                else {{ elementary.edr_timeadd(metric_properties.time_bucket.period, 'periods_until_max', 'last_max_bucket_end') }} {# Add full buckets to last_max_bucket_end #}
            end as max_bucket_end
        from full_buckets_calc
    {%- endset %}

    {%- set bucket_times_query_result = elementary.agate_to_dicts(run_query(bucket_times_query)) %}

    {%- if bucket_times_query_result %}
        {{ return(bucket_times_query_result) }}
    {%- else %}
        {{ return(min_bucket_start) }}
    {%- endif %}

{% endmacro %}