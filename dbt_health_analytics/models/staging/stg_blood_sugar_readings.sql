{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw', 'blood_sugar_readings') }}
),

transformed as (
    select
        -- Primary key
        reading_id,
        
        -- Foreign key
        checkup_id,
        
        -- Measurements
        cast(glucose_level as decimal(6,2)) as glucose_level,
        lower(trim(measurement_type)) as measurement_type,
        measurement_time,
        trim(notes) as notes,
        
        -- Glucose level categorization based on measurement type and ADA guidelines
        case 
            when measurement_type = 'fasting' then
                case 
                    when glucose_level < 70 then 'Hypoglycemia'
                    when glucose_level between 70 and 99 then 'Normal'
                    when glucose_level between 100 and 125 then 'Prediabetes'
                    when glucose_level >= 126 then 'Diabetes'
                    else 'Unknown'
                end
            when measurement_type = 'random' then
                case 
                    when glucose_level < 70 then 'Hypoglycemia'
                    when glucose_level < 140 then 'Normal'
                    when glucose_level between 140 and 199 then 'Prediabetes'
                    when glucose_level >= 200 then 'Diabetes'
                    else 'Unknown'
                end
            when measurement_type = 'post_meal' then
                case 
                    when glucose_level < 70 then 'Hypoglycemia'
                    when glucose_level < 140 then 'Normal'
                    when glucose_level between 140 and 199 then 'Elevated'
                    when glucose_level >= 200 then 'High'
                    else 'Unknown'
                end
            when measurement_type = 'oral_glucose_tolerance' then
                case 
                    when glucose_level < 70 then 'Hypoglycemia'
                    when glucose_level < 140 then 'Normal'
                    when glucose_level between 140 and 199 then 'Prediabetes'
                    when glucose_level >= 200 then 'Diabetes'
                    else 'Unknown'
                end
            else 'Unknown'
        end as glucose_category,
        
        -- Risk level assessment
        case 
            when glucose_level < 54 then 'Critical Low'
            when glucose_level < 70 then 'Low'
            when measurement_type = 'fasting' and glucose_level between 70 and 99 then 'Normal'
            when measurement_type = 'fasting' and glucose_level between 100 and 125 then 'Moderate'
            when measurement_type = 'fasting' and glucose_level >= 126 then 'High'
            when measurement_type in ('random', 'post_meal', 'oral_glucose_tolerance') and glucose_level < 140 then 'Normal'
            when measurement_type in ('random', 'post_meal', 'oral_glucose_tolerance') and glucose_level between 140 and 199 then 'Moderate'
            when measurement_type in ('random', 'post_meal', 'oral_glucose_tolerance') and glucose_level >= 200 then 'High'
            when glucose_level > 400 then 'Critical High'
            else 'Unknown'
        end as glucose_risk_level,
        
        -- Normal range indicator
        case 
            when measurement_type = 'fasting' and glucose_level between 70 and 99 then true
            when measurement_type in ('random', 'post_meal', 'oral_glucose_tolerance') and glucose_level < 140 then true
            else false
        end as is_normal_range,
        
        -- Diabetes indicators
        case 
            when measurement_type = 'fasting' and glucose_level >= 126 then true
            when measurement_type in ('random', 'oral_glucose_tolerance') and glucose_level >= 200 then true
            else false
        end as indicates_diabetes,
        
        -- Prediabetes indicators
        case 
            when measurement_type = 'fasting' and glucose_level between 100 and 125 then true
            when measurement_type in ('random', 'oral_glucose_tolerance') and glucose_level between 140 and 199 then true
            else false
        end as indicates_prediabetes,
        
        -- Quality flags
        case 
            when glucose_level < 20 or glucose_level > 600 then true
            when measurement_type not in ('fasting', 'random', 'post_meal', 'oral_glucose_tolerance') then true
            else false
        end as data_quality_flag,
        
        -- Metadata
        created_at,
        current_timestamp as dbt_updated_at
        
    from source_data
    where glucose_level is not null 
      and measurement_type is not null
)

select * from transformed