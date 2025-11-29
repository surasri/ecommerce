{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        on_schema_change='append_new_columns'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_order_items') }}
    
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        -- Primary keys
        order_item_id,
        order_id,
        product_id,
        
        -- Quantities and pricing
        quantity,
        unit_price,
        COALESCE(discount_percent, 0) AS discount_percent,
        line_total,
        
        -- Calculated fields
        ROUND(unit_price * quantity, 2) AS gross_line_total,
        ROUND(unit_price * quantity * (discount_percent / 100.0), 2) AS discount_amount,
        ROUND(line_total / NULLIF(quantity, 0), 2) AS effective_unit_price,
        
        -- Data quality checks
        CASE 
            WHEN ABS(line_total - (unit_price * quantity * (1 - discount_percent / 100.0))) > 0.01 
            THEN 1 
            ELSE 0 
        END AS has_price_mismatch,
        
        -- Audit fields
        _loaded_at,
        _source_file,
        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM source
    WHERE order_item_id IS NOT NULL
        AND order_id IS NOT NULL
        AND product_id IS NOT NULL
        AND quantity > 0
)

SELECT * FROM cleaned