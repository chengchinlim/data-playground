{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw', 'sodium_readings') }}
),

transformed as (
    select
        -- Primary key
        reading_id,
        
        -- Foreign key
        checkup_id,
        
        -- Measurements
        cast(sodium_level as decimal(6,2)) as sodium_level,
        coalesce(trim(test_type), 'Standard') as test_type,
        measurement_time,
        trim(notes) as notes,
        
        -- Sodium level categorization (Normal range: 135-145 mEq/L)
        case 
            when sodium_level < 135 then 'Hyponatremia'
            when sodium_level between 135 and 145 then 'Normal'
            when sodium_level > 145 then 'Hypernatremia'
            else 'Unknown'
        end as sodium_category,
        
        -- Severity classification for abnormal levels
        case 
            when sodium_level < 125 then 'Severe Hyponatremia'
            when sodium_level between 125 and 134 then 'Mild-Moderate Hyponatremia'
            when sodium_level between 135 and 145 then 'Normal'
            when sodium_level between 146 and 160 then 'Mild-Moderate Hypernatremia'
            when sodium_level > 160 then 'Severe Hypernatremia'
            else 'Unknown'
        end as severity_category,
        
        -- Clinical risk assessment
        case 
            when sodium_level < 120 or sodium_level > 170 then 'Critical'
            when sodium_level < 125 or sodium_level > 160 then 'High'
            when sodium_level < 130 or sodium_level > 150 then 'Moderate'
            when sodium_level between 130 and 150 then 'Low'
            else 'Unknown'
        end as clinical_risk_level,
        
        -- Normal range indicator
        case 
            when sodium_level between 135 and 145 then true
            else false
        end as is_normal_range,
        
        -- Deviation from normal range
        case 
            when sodium_level < 135 then cast(135 - sodium_level as decimal(6,2))
            when sodium_level > 145 then cast(sodium_level - 145 as decimal(6,2))
            else 0
        end as deviation_from_normal,
        
        -- Treatment urgency indicators
        case 
            when sodium_level < 120 then 'Immediate Emergency Care'
            when sodium_level < 125 then 'Urgent Medical Attention'
            when sodium_level < 130 then 'Prompt Medical Evaluation'
            when sodium_level > 170 then 'Immediate Emergency Care'
            when sodium_level > 160 then 'Urgent Medical Attention'
            when sodium_level > 150 then 'Medical Evaluation Recommended'
            when sodium_level between 135 and 145 then 'Routine Monitoring'
            else 'Medical Review Recommended'
        end as treatment_urgency,
        
        -- Potential causes indicators
        case 
            when sodium_level < 135 then 'Possible causes: Excessive water intake, SIADH, heart failure, kidney disease, medications'
            when sodium_level > 145 then 'Possible causes: Dehydration, excessive salt intake, diabetes insipidus, kidney disease'
            else 'Normal sodium balance'
        end as potential_causes,
        
        -- Monitoring recommendations
        case 
            when sodium_level < 125 or sodium_level > 160 then 'Monitor every 2-4 hours until stable'
            when sodium_level < 130 or sodium_level > 150 then 'Monitor daily until normalized'
            when sodium_level between 130 and 134 or sodium_level between 146 and 150 then 'Monitor every 2-3 days'
            when sodium_level between 135 and 145 then 'Routine monitoring as per schedule'
            else 'Consult healthcare provider for monitoring frequency'
        end as monitoring_recommendation,
        
        -- Quality flags
        case 
            when sodium_level < 100 or sodium_level > 200 then true
            when test_type not in ('Standard', 'Basic Metabolic Panel', 'Comprehensive Metabolic Panel', 'Electrolyte Panel') and test_type is not null then true
            else false
        end as data_quality_flag,
        
        -- Distance from optimal range (middle of normal range: 140 mEq/L)
        abs(sodium_level - 140) as distance_from_optimal,
        
        -- Percentile within normal range (for normal values only)
        case 
            when sodium_level between 135 and 145 then 
                cast(((sodium_level - 135) / (145 - 135)) * 100 as decimal(5,2))
            else null
        end as normal_range_percentile,
        
        -- Metadata
        created_at,
        current_timestamp as dbt_updated_at
        
    from source_data
    where sodium_level is not null
)

select * from transformed