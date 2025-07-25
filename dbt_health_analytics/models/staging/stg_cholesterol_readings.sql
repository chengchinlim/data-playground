{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw', 'cholesterol_readings') }}
),

transformed as (
    select
        -- Primary key
        reading_id,
        
        -- Foreign key
        checkup_id,
        
        -- Measurements
        cast(total_cholesterol as decimal(6,2)) as total_cholesterol,
        cast(ldl_cholesterol as decimal(6,2)) as ldl_cholesterol,
        cast(hdl_cholesterol as decimal(6,2)) as hdl_cholesterol,
        cast(triglycerides as decimal(6,2)) as triglycerides,
        fasting_hours,
        measurement_time,
        trim(notes) as notes,
        
        -- Total cholesterol categorization (ATP III guidelines)
        case 
            when total_cholesterol < 200 then 'Desirable'
            when total_cholesterol between 200 and 239 then 'Borderline High'
            when total_cholesterol >= 240 then 'High'
            else 'Unknown'
        end as total_cholesterol_category,
        
        -- LDL cholesterol categorization
        case 
            when ldl_cholesterol < 100 then 'Optimal'
            when ldl_cholesterol between 100 and 129 then 'Near Optimal'
            when ldl_cholesterol between 130 and 159 then 'Borderline High'
            when ldl_cholesterol between 160 and 189 then 'High'
            when ldl_cholesterol >= 190 then 'Very High'
            else 'Unknown'
        end as ldl_cholesterol_category,
        
        -- HDL cholesterol categorization
        case 
            when hdl_cholesterol < 40 then 'Low'
            when hdl_cholesterol between 40 and 59 then 'Moderate'
            when hdl_cholesterol >= 60 then 'High'
            else 'Unknown'
        end as hdl_cholesterol_category,
        
        -- Triglycerides categorization
        case 
            when triglycerides < 150 then 'Normal'
            when triglycerides between 150 and 199 then 'Borderline High'
            when triglycerides between 200 and 499 then 'High'
            when triglycerides >= 500 then 'Very High'
            else 'Unknown'
        end as triglycerides_category,
        
        -- Overall cardiovascular risk assessment
        case 
            when ldl_cholesterol >= 190 or triglycerides >= 500 then 'Very High'
            when ldl_cholesterol >= 160 or (hdl_cholesterol < 40 and triglycerides >= 200) then 'High'
            when ldl_cholesterol >= 130 or hdl_cholesterol < 40 or triglycerides >= 150 then 'Moderate'
            when ldl_cholesterol < 100 and hdl_cholesterol >= 60 and triglycerides < 150 then 'Low'
            else 'Moderate'
        end as cardiovascular_risk_level,
        
        -- Metabolic syndrome indicators
        case 
            when hdl_cholesterol < 40 and triglycerides >= 150 then true
            else false
        end as metabolic_syndrome_indicator,
        
        -- Cholesterol ratio calculations
        case 
            when hdl_cholesterol > 0 then cast(total_cholesterol / hdl_cholesterol as decimal(5,2))
            else null
        end as total_hdl_ratio,
        
        case 
            when hdl_cholesterol > 0 then cast(ldl_cholesterol / hdl_cholesterol as decimal(5,2))
            else null
        end as ldl_hdl_ratio,
        
        -- Risk ratio categorization
        case 
            when hdl_cholesterol > 0 and (total_cholesterol / hdl_cholesterol) < 3.5 then 'Low Risk'
            when hdl_cholesterol > 0 and (total_cholesterol / hdl_cholesterol) between 3.5 and 5.0 then 'Moderate Risk'
            when hdl_cholesterol > 0 and (total_cholesterol / hdl_cholesterol) > 5.0 then 'High Risk'
            else 'Unknown'
        end as ratio_risk_category,
        
        -- Treatment recommendations based on guidelines
        case 
            when ldl_cholesterol >= 190 then 'Statin Recommended'
            when ldl_cholesterol >= 160 and hdl_cholesterol < 40 then 'Consider Statin + Lifestyle'
            when ldl_cholesterol >= 130 or triglycerides >= 200 then 'Lifestyle Changes'
            when ldl_cholesterol < 100 and hdl_cholesterol >= 60 and triglycerides < 150 then 'Continue Current Approach'
            else 'Monitor'
        end as treatment_recommendation,
        
        -- Quality flags
        case 
            when total_cholesterol > 500 or ldl_cholesterol > 400 or hdl_cholesterol > 150 or triglycerides > 2000 then true
            when total_cholesterol < 100 or ldl_cholesterol < 40 or hdl_cholesterol < 20 or triglycerides < 30 then true
            when (ldl_cholesterol + hdl_cholesterol) > total_cholesterol then true
            else false
        end as data_quality_flag,
        
        -- Fasting status validation
        case 
            when fasting_hours >= 9 then 'Adequate Fasting'
            when fasting_hours >= 6 then 'Minimal Fasting'
            when fasting_hours < 6 then 'Insufficient Fasting'
            when fasting_hours is null then 'Unknown Fasting Status'
            else 'Unknown'
        end as fasting_status,
        
        -- Metadata
        created_at,
        current_timestamp as dbt_updated_at
        
    from source_data
    where total_cholesterol is not null 
      and ldl_cholesterol is not null 
      and hdl_cholesterol is not null 
      and triglycerides is not null
)

select * from transformed