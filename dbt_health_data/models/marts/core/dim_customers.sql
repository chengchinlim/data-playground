{{ config(materialized='table') }}

with customers_base as (
    select * from {{ ref('stg_customers') }}
),

checkup_metrics as (
    select 
        customer_id,
        count(*) as total_checkups,
        max(checkup_date) as latest_checkup_date,
        min(checkup_date) as first_checkup_date,
        avg(bmi) as avg_bmi,
        max(case when health_risk_level = 'High' then 1 else 0 end) as has_high_health_risk
    from {{ ref('stg_medical_checkups') }}
    group by customer_id
),

bp_risk_flags as (
    select 
        mc.customer_id,
        max(case when bp.bp_risk_level in ('High', 'Critical') then 1 else 0 end) as has_hypertension_risk,
        max(case when bp.overall_cardiovascular_risk in ('High', 'Critical') then 1 else 0 end) as has_high_cardiovascular_risk
    from {{ ref('stg_medical_checkups') }} mc
    left join {{ ref('stg_blood_pressure_readings') }} bp on mc.checkup_id = bp.checkup_id
    group by mc.customer_id
),

glucose_risk_flags as (
    select 
        mc.customer_id,
        max(case when bg.indicates_diabetes = true then 1 else 0 end) as has_diabetes_indicators,
        max(case when bg.indicates_prediabetes = true then 1 else 0 end) as has_prediabetes_indicators
    from {{ ref('stg_medical_checkups') }} mc
    left join {{ ref('stg_blood_sugar_readings') }} bg on mc.checkup_id = bg.checkup_id
    group by mc.customer_id
),

cholesterol_risk_flags as (
    select 
        mc.customer_id,
        max(case when chol.cardiovascular_risk_level in ('High', 'Very High') then 1 else 0 end) as has_high_cholesterol_risk
    from {{ ref('stg_medical_checkups') }} mc
    left join {{ ref('stg_cholesterol_readings') }} chol on mc.checkup_id = chol.checkup_id
    group by mc.customer_id
),

final as (
    select
        -- Customer identifiers and demographics
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.date_of_birth,
        c.gender,
        c.age_years,
        c.age_group,
        
        -- Health engagement metrics
        coalesce(cm.total_checkups, 0) as total_checkups,
        cm.latest_checkup_date,
        cm.first_checkup_date,
        case 
            when cm.latest_checkup_date is null then 'Never'
            when cm.latest_checkup_date >= current_date - interval '3 months' then 'Recent'
            when cm.latest_checkup_date >= current_date - interval '6 months' then 'Moderate'
            when cm.latest_checkup_date >= current_date - interval '12 months' then 'Overdue'
            else 'Inactive'
        end as engagement_status,
        
        -- Health profile summary
        round(coalesce(cm.avg_bmi, 0), 2) as avg_bmi,
        case 
            when cm.avg_bmi < 18.5 then 'Underweight'
            when cm.avg_bmi between 18.5 and 24.9 then 'Normal weight'
            when cm.avg_bmi between 25.0 and 29.9 then 'Overweight'
            when cm.avg_bmi >= 30.0 then 'Obese'
            else 'Unknown'
        end as avg_bmi_category,
        
        -- Risk indicators (boolean flags)
        coalesce(cm.has_high_health_risk, 0) as has_high_health_risk,
        coalesce(bp.has_hypertension_risk, 0) as has_hypertension_risk,
        coalesce(bp.has_high_cardiovascular_risk, 0) as has_high_cardiovascular_risk,
        coalesce(gf.has_diabetes_indicators, 0) as has_diabetes_indicators,
        coalesce(gf.has_prediabetes_indicators, 0) as has_prediabetes_indicators,
        coalesce(cf.has_high_cholesterol_risk, 0) as has_high_cholesterol_risk,
        
        -- Overall risk score (sum of risk flags)
        (coalesce(cm.has_high_health_risk, 0) + 
         coalesce(bp.has_hypertension_risk, 0) + 
         coalesce(bp.has_high_cardiovascular_risk, 0) + 
         coalesce(gf.has_diabetes_indicators, 0) + 
         coalesce(gf.has_prediabetes_indicators, 0) + 
         coalesce(cf.has_high_cholesterol_risk, 0)) as total_risk_flags,
         
        -- Risk category
        case 
            when (coalesce(cm.has_high_health_risk, 0) + 
                  coalesce(bp.has_hypertension_risk, 0) + 
                  coalesce(bp.has_high_cardiovascular_risk, 0) + 
                  coalesce(gf.has_diabetes_indicators, 0) + 
                  coalesce(gf.has_prediabetes_indicators, 0) + 
                  coalesce(cf.has_high_cholesterol_risk, 0)) = 0 then 'Low Risk'
            when (coalesce(cm.has_high_health_risk, 0) + 
                  coalesce(bp.has_hypertension_risk, 0) + 
                  coalesce(bp.has_high_cardiovascular_risk, 0) + 
                  coalesce(gf.has_diabetes_indicators, 0) + 
                  coalesce(gf.has_prediabetes_indicators, 0) + 
                  coalesce(cf.has_high_cholesterol_risk, 0)) between 1 and 2 then 'Moderate Risk'
            when (coalesce(cm.has_high_health_risk, 0) + 
                  coalesce(bp.has_hypertension_risk, 0) + 
                  coalesce(bp.has_high_cardiovascular_risk, 0) + 
                  coalesce(gf.has_diabetes_indicators, 0) + 
                  coalesce(gf.has_prediabetes_indicators, 0) + 
                  coalesce(cf.has_high_cholesterol_risk, 0)) >= 3 then 'High Risk'
            else 'Unknown'
        end as overall_risk_category,
        
        -- Customer tenure
        case 
            when cm.first_checkup_date is not null 
            then date_diff('day', cm.first_checkup_date, coalesce(cm.latest_checkup_date, current_date))
            else null
        end as days_as_patient,
        
        -- Metadata
        c.created_at as customer_created_at,
        current_timestamp as dbt_updated_at
        
    from customers_base c
    left join checkup_metrics cm on c.customer_id = cm.customer_id
    left join bp_risk_flags bp on c.customer_id = bp.customer_id
    left join glucose_risk_flags gf on c.customer_id = gf.customer_id
    left join cholesterol_risk_flags cf on c.customer_id = cf.customer_id
)

select * from final