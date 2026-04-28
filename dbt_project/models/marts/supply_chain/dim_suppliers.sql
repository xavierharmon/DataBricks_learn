-- models/marts/supply_chain/dim_suppliers.sql
-- MART MODEL: dim_suppliers
-- GRAIN: One row per supplier (current state)
-- PURPOSE: Supplier dimension for supply chain reporting

with source as (
    select * from {{ ref('int_supplier_performance') }}
)

select
    -- Keys
    supplier_id,
    {{ dbt_utils.generate_surrogate_key(['supplier_id']) }}     as supplier_sk,

    -- Identity
    supplier_name,
    country,
    state,
    supplier_type,
    is_preferred,
    onboarded_at,

    -- Contract terms
    quoted_lead_time_days,
    quoted_fill_rate,
    payment_terms_days,
    quality_rating,

    -- Material coverage
    materials_supplied,
    primary_material_count,

    -- PO history
    total_pos,
    received_pos,
    cancelled_pos,
    first_po_date,
    most_recent_po_date,
    total_po_spend,

    -- Performance metrics
    on_time_deliveries,
    late_deliveries,
    avg_days_late,
    avg_actual_lead_time_days,
    on_time_delivery_rate,
    actual_fill_rate,
    avg_cost_variance_pct,

    -- Scoring
    supplier_score,
    performance_tier,

    -- Derived flags
    case when performance_tier in ('gold', 'silver') then 1 else 0 end
                                                                as is_high_performing,
    case when performance_tier = 'probation' then 1 else 0 end
                                                                as is_at_risk,
    case when avg_days_late > 5 then 1 else 0 end               as is_chronically_late,

    {{ current_timestamp_fn() }}                                as dbt_updated_at

from source
