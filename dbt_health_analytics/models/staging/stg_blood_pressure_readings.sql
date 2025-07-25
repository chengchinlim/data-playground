{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw', 'blood_pressure_readings') }}
),

transformed as (
    select
        -- Primary key
        reading_id,
        
        -- Foreign key
        checkup_id,
        
        -- Measurements
        cast(systolic_pressure as integer) as systolic_pressure,
        cast(diastolic_pressure as integer) as diastolic_pressure,
        cast(heart_rate as integer) as heart_rate,
        measurement_time,
        trim(notes) as notes,
        
        -- Blood pressure categorization (American Heart Association guidelines)
        case 
            when systolic_pressure < 120 and diastolic_pressure < 80 then 'Normal'
            when (systolic_pressure between 120 and 129) and diastolic_pressure < 80 then 'Elevated'
            when (systolic_pressure between 130 and 139) or (diastolic_pressure between 80 and 89) then 'High Blood Pressure Stage 1'
            when (systolic_pressure between 140 and 179) or (diastolic_pressure between 90 and 119) then 'High Blood Pressure Stage 2'
            when systolic_pressure >= 180 or diastolic_pressure >= 120 then 'Hypertensive Crisis'
            else 'Unknown'
        end as bp_category,
        
        -- Risk level based on blood pressure
        case 
            when systolic_pressure < 120 and diastolic_pressure < 80 then 'Low'
            when (systolic_pressure between 120 and 129) and diastolic_pressure < 80 then 'Low-Moderate'
            when (systolic_pressure between 130 and 139) or (diastolic_pressure between 80 and 89) then 'Moderate'
            when (systolic_pressure between 140 and 179) or (diastolic_pressure between 90 and 119) then 'High'
            when systolic_pressure >= 180 or diastolic_pressure >= 120 then 'Critical'
            else 'Unknown'
        end as bp_risk_level,
        
        -- Heart rate categorization (adult resting heart rate)
        case 
            when heart_rate < 60 then 'Bradycardia'
            when heart_rate between 60 and 100 then 'Normal'
            when heart_rate > 100 then 'Tachycardia'
            else 'Unknown'
        end as heart_rate_category,
        
        -- Combined risk assessment
        case 
            when (systolic_pressure >= 180 or diastolic_pressure >= 120) or heart_rate > 120 then 'Critical'
            when (systolic_pressure >= 140 or diastolic_pressure >= 90) or heart_rate < 50 or heart_rate > 110 then 'High'
            when (systolic_pressure >= 130 or diastolic_pressure >= 80) or heart_rate < 55 or heart_rate > 105 then 'Moderate'
            when (systolic_pressure >= 120 and diastolic_pressure < 80) or heart_rate < 60 or heart_rate > 100 then 'Low-Moderate'
            when systolic_pressure < 120 and diastolic_pressure < 80 and heart_rate between 60 and 100 then 'Low'
            else 'Unknown'
        end as overall_cardiovascular_risk,
        
        -- Quality flags
        case 
            when systolic_pressure <= diastolic_pressure then true
            when systolic_pressure > 300 or diastolic_pressure > 200 then true
            when systolic_pressure < 70 or diastolic_pressure < 40 then true
            when heart_rate < 30 or heart_rate > 250 then true
            else false
        end as data_quality_flag,
        
        -- Metadata
        created_at,
        current_timestamp as dbt_updated_at
        
    from source_data
    where systolic_pressure is not null 
      and diastolic_pressure is not null 
      and heart_rate is not null
)

select * from transformed