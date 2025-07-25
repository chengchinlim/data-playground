{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw', 'medical_checkups') }}
),

transformed as (
    select
        -- Primary key
        checkup_id,
        
        -- Foreign key
        customer_id,
        
        -- Checkup details
        checkup_date,
        coalesce(checkup_type, 'General') as checkup_type,
        trim(notes) as notes,
        
        -- Measurements
        cast(height_cm as decimal(5,2)) as height_cm,
        cast(weight_kg as decimal(5,2)) as weight_kg,
        
        -- Calculated BMI
        cast(weight_kg / power(height_cm / 100.0, 2) as decimal(5,2)) as bmi,
        
        -- BMI categorization
        case 
            when (weight_kg / power(height_cm / 100.0, 2)) < 18.5 then 'Underweight'
            when (weight_kg / power(height_cm / 100.0, 2)) between 18.5 and 24.9 then 'Normal weight'
            when (weight_kg / power(height_cm / 100.0, 2)) between 25.0 and 29.9 then 'Overweight'
            when (weight_kg / power(height_cm / 100.0, 2)) between 30.0 and 34.9 then 'Obesity Class I'
            when (weight_kg / power(height_cm / 100.0, 2)) between 35.0 and 39.9 then 'Obesity Class II'
            when (weight_kg / power(height_cm / 100.0, 2)) >= 40.0 then 'Obesity Class III'
            else 'Unknown'
        end as bmi_category,
        
        -- Risk indicators
        case 
            when (weight_kg / power(height_cm / 100.0, 2)) < 18.5 then 'High'
            when (weight_kg / power(height_cm / 100.0, 2)) between 18.5 and 24.9 then 'Low'
            when (weight_kg / power(height_cm / 100.0, 2)) between 25.0 and 29.9 then 'Moderate'
            when (weight_kg / power(height_cm / 100.0, 2)) >= 30.0 then 'High'
            else 'Unknown'
        end as health_risk_level,
        
        -- Metadata
        created_at,
        current_timestamp as dbt_updated_at
        
    from source_data
    where height_cm > 0 and weight_kg > 0  -- Data quality filter
)

select * from transformed