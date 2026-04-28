-- macros/platform_adapter.sql
-- -----------------------------------------------------------------------
-- PLATFORM ADAPTER MACROS
-- -----------------------------------------------------------------------
-- This is the KEY FILE that makes the project work on both platforms.
--
-- PROBLEM:
--   Databricks uses Spark SQL dialect.
--   Azure Fabric uses T-SQL dialect (like SQL Server).
--   Some functions have different names or syntax between the two.
--
-- SOLUTION:
--   Every function that differs between platforms gets a macro here.
--   Models call the macro — never the platform-specific function directly.
--   When you switch platforms, the macros automatically emit the right SQL.
--
-- HOW IT WORKS:
--   {{ current_timestamp_fn() }}
--   On Databricks → compiles to: current_timestamp()
--   On Fabric     → compiles to: getdate()
--
--   The `target.type` variable is set automatically by dbt based on
--   which profile you're using. No manual flag needed.
-- -----------------------------------------------------------------------


-- -----------------------------------------------------------------------
-- CURRENT TIMESTAMP
-- Databricks: current_timestamp()
-- Fabric:     getdate()
-- -----------------------------------------------------------------------
{% macro current_timestamp_fn() %}
  {% if target.type == 'fabric' or target.type == 'sqlserver' %}
    getdate()
  {% else %}
    current_timestamp()
  {% endif %}
{% endmacro %}


-- -----------------------------------------------------------------------
-- CURRENT DATE
-- Databricks: current_date()
-- Fabric:     cast(getdate() as date)
-- -----------------------------------------------------------------------
{% macro current_date_fn() %}
  {% if target.type == 'fabric' or target.type == 'sqlserver' %}
    cast(getdate() as date)
  {% else %}
    current_date()
  {% endif %}
{% endmacro %}


-- -----------------------------------------------------------------------
-- DATE TRUNCATION
-- Databricks: date_trunc('month', date_col)
-- Fabric:     dateadd(month, datediff(month, 0, date_col), 0)
--
-- Usage: {{ date_trunc_fn('month', 'order_date') }}
-- -----------------------------------------------------------------------
{% macro date_trunc_fn(period, date_col) %}
  {% if target.type == 'fabric' or target.type == 'sqlserver' %}
    {% if period == 'month' %}
      dateadd(month, datediff(month, 0, {{ date_col }}), 0)
    {% elif period == 'week' %}
      dateadd(week, datediff(week, 0, {{ date_col }}), 0)
    {% elif period == 'year' %}
      dateadd(year, datediff(year, 0, {{ date_col }}), 0)
    {% elif period == 'day' %}
      cast({{ date_col }} as date)
    {% endif %}
  {% else %}
    date_trunc('{{ period }}', {{ date_col }})
  {% endif %}
{% endmacro %}


-- -----------------------------------------------------------------------
-- YEAR EXTRACTION
-- Databricks: year(date_col)
-- Fabric:     year(date_col)   ← same! included for completeness
-- -----------------------------------------------------------------------
{% macro year_fn(date_col) %}
  year({{ date_col }})
{% endmacro %}


-- -----------------------------------------------------------------------
-- DAY OF WEEK
-- Databricks: dayofweek(date_col)   → 1=Sunday, 7=Saturday
-- Fabric:     datepart(weekday, date_col) → 1=Sunday, 7=Saturday
-- -----------------------------------------------------------------------
{% macro dayofweek_fn(date_col) %}
  {% if target.type == 'fabric' or target.type == 'sqlserver' %}
    datepart(weekday, {{ date_col }})
  {% else %}
    dayofweek({{ date_col }})
  {% endif %}
{% endmacro %}


-- -----------------------------------------------------------------------
-- DATE DIFFERENCE (days between two dates)
-- Databricks: datediff(end_date, start_date)
-- Fabric:     datediff(day, start_date, end_date)   ← note argument order flip!
-- -----------------------------------------------------------------------
{% macro datediff_fn(start_date, end_date) %}
  {% if target.type == 'fabric' or target.type == 'sqlserver' %}
    datediff(day, {{ start_date }}, {{ end_date }})
  {% else %}
    datediff({{ end_date }}, {{ start_date }})
  {% endif %}
{% endmacro %}


-- -----------------------------------------------------------------------
-- DATE ADD (add N days to a date)
-- Databricks: dateadd(day, -30, current_date())
-- Fabric:     dateadd(day, -30, getdate())          ← same function, different date fn
-- -----------------------------------------------------------------------
{% macro dateadd_fn(period, number, date_col) %}
  dateadd({{ period }}, {{ number }}, {{ date_col }})
{% endmacro %}


-- -----------------------------------------------------------------------
-- SPLIT STRING (used in stg_customers to split full_name)
-- Databricks: split_part(string, delimiter, position)
-- Fabric:     No split_part — must use charindex + substring
-- -----------------------------------------------------------------------
{% macro split_first_name(full_name_col) %}
  {% if target.type == 'fabric' or target.type == 'sqlserver' %}
    ltrim(rtrim(
      case
        when charindex(' ', {{ full_name_col }}) > 0
        then substring({{ full_name_col }}, 1, charindex(' ', {{ full_name_col }}) - 1)
        else {{ full_name_col }}
      end
    ))
  {% else %}
    trim(split_part({{ full_name_col }}, ' ', 1))
  {% endif %}
{% endmacro %}

{% macro split_last_name(full_name_col) %}
  {% if target.type == 'fabric' or target.type == 'sqlserver' %}
    ltrim(rtrim(
      case
        when charindex(' ', {{ full_name_col }}) > 0
        then substring({{ full_name_col }}, charindex(' ', {{ full_name_col }}) + 1, len({{ full_name_col }}))
        else ''
      end
    ))
  {% else %}
    trim(split_part({{ full_name_col }}, ' ', 2))
  {% endif %}
{% endmacro %}


-- -----------------------------------------------------------------------
-- BOOLEAN CASTING
-- Databricks: column::boolean  or  true/false literals
-- Fabric:     cast(column as bit)  or  1/0 literals
--
-- Fabric doesn't have a true BOOLEAN type — it uses BIT (1/0).
-- This macro normalizes boolean expressions across platforms.
-- -----------------------------------------------------------------------
{% macro bool_to_bit(expression) %}
  {% if target.type == 'fabric' or target.type == 'sqlserver' %}
    cast(case when {{ expression }} then 1 else 0 end as bit)
  {% else %}
    {{ expression }}
  {% endif %}
{% endmacro %}


-- -----------------------------------------------------------------------
-- SAFE CAST — cast with a fallback on failure
-- Databricks: try_cast(value as type)
-- Fabric:     try_cast(value as type)   ← same in modern Fabric! 
-- -----------------------------------------------------------------------
{% macro safe_cast(value, type) %}
  try_cast({{ value }} as {{ type }})
{% endmacro %}
