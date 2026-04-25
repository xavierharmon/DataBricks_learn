-- dbt_project/macros/fiscal_quarter.sql
-- -----------------------------------------------------------------------
-- MACRO: fiscal_quarter
-- -----------------------------------------------------------------------
-- WHAT IS A dbt MACRO?
--   A macro is a reusable SQL snippet written in Jinja2 (the templating
--   language dbt uses). Think of it like a SQL function — you define it
--   once and call it anywhere in your models.
--
-- WHY USE MACROS?
--   Without macros:
--     Every model that needs fiscal quarter logic copies the same CASE statement.
--     When the fiscal year definition changes, you update 15 files.
--
--   With macros:
--     One definition. Called everywhere. One place to update.
--
-- HOW TO CALL THIS MACRO IN A MODEL:
--   {{ fiscal_quarter('order_date') }}
--
--   dbt replaces that call with the full CASE expression at compile time.
-- -----------------------------------------------------------------------

{% macro fiscal_quarter(date_column) %}
-- Assumes fiscal year starts in February (common in retail)
-- Q1 = Feb-Apr, Q2 = May-Jul, Q3 = Aug-Oct, Q4 = Nov-Jan
case
    when month({{ date_column }}) in (2, 3, 4)   then 'Q1'
    when month({{ date_column }}) in (5, 6, 7)   then 'Q2'
    when month({{ date_column }}) in (8, 9, 10)  then 'Q3'
    when month({{ date_column }}) in (11, 12, 1) then 'Q4'
end
{% endmacro %}


-- -----------------------------------------------------------------------
-- MACRO: cents_to_dollars
-- -----------------------------------------------------------------------
-- Some source systems store monetary values in CENTS (integers) to avoid
-- floating point precision issues. This macro converts them to dollars.
--
-- Usage: {{ cents_to_dollars('amount_in_cents') }}
-- -----------------------------------------------------------------------

{% macro cents_to_dollars(column_name, precision=2) %}
round({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}


-- -----------------------------------------------------------------------
-- MACRO: safe_divide
-- -----------------------------------------------------------------------
-- Division in SQL fails silently (returns NULL) or errors on divide-by-zero.
-- This macro wraps division with a null-safe guard.
--
-- Usage: {{ safe_divide('revenue', 'order_count') }}
-- -----------------------------------------------------------------------

{% macro safe_divide(numerator, denominator, default=0) %}
case
    when {{ denominator }} = 0 or {{ denominator }} is null
    then {{ default }}
    else {{ numerator }} / {{ denominator }}
end
{% endmacro %}