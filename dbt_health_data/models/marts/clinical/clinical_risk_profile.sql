{{ config(materialized='table') }}

with customers_base as (
    select * from {{ ref('dim_customers') }}
),

latest_checkups as (
    select 
        customer_id,
        checkup_id,
        checkup_date,
        bmi,
        bmi_category,
        health_risk_level,
        row_number() over (partition by customer_id order by checkup_date desc) as rn
    from {{ ref('stg_medical_checkups') }}
),

latest_bp_readings as (
    select 
        mc.customer_id,
        bp.systolic_pressure,
        bp.diastolic_pressure,
        bp.heart_rate,
        bp.bp_category,
        bp.bp_risk_level,
        bp.heart_rate_category,
        bp.overall_cardiovascular_risk as bp_cardiovascular_risk,
        row_number() over (partition by mc.customer_id order by mc.checkup_date desc, bp.reading_id desc) as rn
    from {{ ref('stg_medical_checkups') }} mc
    inner join {{ ref('stg_blood_pressure_readings') }} bp on mc.checkup_id = bp.checkup_id
),

latest_glucose_readings as (
    select 
        mc.customer_id,
        bg.glucose_level,
        bg.measurement_type,
        bg.glucose_category,
        bg.glucose_risk_level,
        bg.is_normal_range as glucose_normal_range,
        bg.indicates_diabetes,
        bg.indicates_prediabetes,
        row_number() over (partition by mc.customer_id order by mc.checkup_date desc, bg.reading_id desc) as rn
    from {{ ref('stg_medical_checkups') }} mc
    inner join {{ ref('stg_blood_sugar_readings') }} bg on mc.checkup_id = bg.checkup_id
),

latest_cholesterol_readings as (
    select 
        mc.customer_id,
        chol.total_cholesterol,
        chol.ldl_cholesterol,
        chol.hdl_cholesterol,
        chol.triglycerides,
        chol.total_cholesterol_category,
        chol.ldl_cholesterol_category,
        chol.hdl_cholesterol_category,
        chol.triglycerides_category,
        chol.cardiovascular_risk_level as cholesterol_cardiovascular_risk,
        chol.total_hdl_ratio,
        chol.treatment_recommendation,
        row_number() over (partition by mc.customer_id order by mc.checkup_date desc, chol.reading_id desc) as rn
    from {{ ref('stg_medical_checkups') }} mc
    inner join {{ ref('stg_cholesterol_readings') }} chol on mc.checkup_id = chol.checkup_id
),

latest_sodium_readings as (
    select 
        mc.customer_id,
        sod.sodium_level,
        sod.sodium_category,
        sod.severity_category,
        sod.clinical_risk_level as sodium_risk_level,
        sod.is_normal_range as sodium_normal_range,
        sod.treatment_urgency,
        row_number() over (partition by mc.customer_id order by mc.checkup_date desc, sod.reading_id desc) as rn
    from {{ ref('stg_medical_checkups') }} mc
    inner join {{ ref('stg_sodium_readings') }} sod on mc.checkup_id = sod.checkup_id
),

risk_scoring as (
    select
        c.customer_id,
        c.age_years,
        c.gender,
        
        -- Latest checkup data
        lc.checkup_date as latest_checkup_date,
        lc.bmi,
        lc.bmi_category,
        lc.health_risk_level as bmi_health_risk,
        
        -- Blood pressure metrics
        bp.systolic_pressure,
        bp.diastolic_pressure,
        bp.heart_rate,
        bp.bp_category,
        bp.bp_risk_level,
        bp.bp_cardiovascular_risk,
        
        -- Glucose metrics
        gluc.glucose_level,
        gluc.measurement_type as glucose_measurement_type,
        gluc.glucose_category,
        gluc.glucose_risk_level,
        gluc.indicates_diabetes,
        gluc.indicates_prediabetes,
        
        -- Cholesterol metrics
        chol.total_cholesterol,
        chol.ldl_cholesterol,
        chol.hdl_cholesterol,
        chol.triglycerides,
        chol.cholesterol_cardiovascular_risk,
        chol.total_hdl_ratio,
        chol.treatment_recommendation,
        
        -- Sodium metrics
        sod.sodium_level,
        sod.sodium_category,
        sod.sodium_risk_level,
        
        -- Individual risk scores (0=low, 1=moderate, 2=high, 3=critical)
        case 
            when lc.health_risk_level = 'Low' then 0
            when lc.health_risk_level = 'Moderate' then 1
            when lc.health_risk_level = 'High' then 2
            else 0
        end as bmi_risk_score,
        
        case 
            when bp.bp_risk_level in ('Low', 'Low-Moderate') then 0
            when bp.bp_risk_level = 'Moderate' then 1
            when bp.bp_risk_level = 'High' then 2
            when bp.bp_risk_level = 'Critical' then 3
            else 0
        end as bp_risk_score,
        
        case 
            when gluc.glucose_risk_level in ('Low', 'Normal') then 0
            when gluc.glucose_risk_level = 'Moderate' then 1
            when gluc.glucose_risk_level = 'High' then 2
            when gluc.glucose_risk_level in ('Critical High', 'Critical Low') then 3
            else 0
        end as glucose_risk_score,
        
        case 
            when chol.cholesterol_cardiovascular_risk = 'Low' then 0
            when chol.cholesterol_cardiovascular_risk = 'Moderate' then 1
            when chol.cholesterol_cardiovascular_risk = 'High' then 2
            when chol.cholesterol_cardiovascular_risk = 'Very High' then 3
            else 0
        end as cholesterol_risk_score,
        
        case 
            when sod.sodium_risk_level = 'Low' then 0
            when sod.sodium_risk_level = 'Moderate' then 1
            when sod.sodium_risk_level = 'High' then 2
            when sod.sodium_risk_level = 'Critical' then 3
            else 0
        end as sodium_risk_score
        
    from customers_base c
    left join latest_checkups lc on c.customer_id = lc.customer_id and lc.rn = 1
    left join latest_bp_readings bp on c.customer_id = bp.customer_id and bp.rn = 1
    left join latest_glucose_readings gluc on c.customer_id = gluc.customer_id and gluc.rn = 1
    left join latest_cholesterol_readings chol on c.customer_id = chol.customer_id and chol.rn = 1
    left join latest_sodium_readings sod on c.customer_id = sod.customer_id and sod.rn = 1
),

final as (
    select
        customer_id,
        age_years,
        gender,
        latest_checkup_date,
        
        -- Physical health metrics
        bmi,
        bmi_category,
        
        -- Vital signs
        systolic_pressure,
        diastolic_pressure,
        heart_rate,
        bp_category,
        
        -- Laboratory values
        glucose_level,
        glucose_measurement_type,
        glucose_category,
        total_cholesterol,
        ldl_cholesterol,
        hdl_cholesterol,
        triglycerides,
        total_hdl_ratio,
        sodium_level,
        sodium_category,
        
        -- Individual risk assessments
        bmi_health_risk,
        bp_risk_level,
        glucose_risk_level,
        cholesterol_cardiovascular_risk,
        sodium_risk_level,
        
        -- Clinical indicators
        indicates_diabetes,
        indicates_prediabetes,
        treatment_recommendation,
        
        -- Risk scoring
        bmi_risk_score,
        bp_risk_score,
        glucose_risk_score,
        cholesterol_risk_score,
        sodium_risk_score,
        
        -- Composite risk score (weighted average)
        round(
            (bmi_risk_score * 0.15 + 
             bp_risk_score * 0.25 + 
             glucose_risk_score * 0.25 + 
             cholesterol_risk_score * 0.25 + 
             sodium_risk_score * 0.10), 2
        ) as composite_risk_score,
        
        -- Overall risk category
        case 
            when round(
                (bmi_risk_score * 0.15 + 
                 bp_risk_score * 0.25 + 
                 glucose_risk_score * 0.25 + 
                 cholesterol_risk_score * 0.25 + 
                 sodium_risk_score * 0.10), 2
            ) < 0.5 then 'Low Risk'
            when round(
                (bmi_risk_score * 0.15 + 
                 bp_risk_score * 0.25 + 
                 glucose_risk_score * 0.25 + 
                 cholesterol_risk_score * 0.25 + 
                 sodium_risk_score * 0.10), 2
            ) < 1.5 then 'Moderate Risk'
            when round(
                (bmi_risk_score * 0.15 + 
                 bp_risk_score * 0.25 + 
                 glucose_risk_score * 0.25 + 
                 cholesterol_risk_score * 0.25 + 
                 sodium_risk_score * 0.10), 2
            ) < 2.5 then 'High Risk'
            else 'Critical Risk'
        end as overall_risk_category,
        
        -- Clinical recommendations
        case
            when indicates_diabetes = true then 'Diabetes Management Required'
            when indicates_prediabetes = true then 'Prediabetes Monitoring Required'
            when bp_risk_level in ('High', 'Critical') then 'Hypertension Management Required'
            when cholesterol_cardiovascular_risk in ('High', 'Very High') then 'Cholesterol Management Required'
            when sodium_risk_level in ('High', 'Critical') then 'Electrolyte Management Required'
            when bmi_health_risk = 'High' then 'Weight Management Recommended'
            else 'Continue Routine Monitoring'
        end as primary_clinical_recommendation,
        
        -- Care priority
        case
            when round(
                (bmi_risk_score * 0.15 + 
                 bp_risk_score * 0.25 + 
                 glucose_risk_score * 0.25 + 
                 cholesterol_risk_score * 0.25 + 
                 sodium_risk_score * 0.10), 2
            ) >= 2.5 then 'Immediate Care'
            when round(
                (bmi_risk_score * 0.15 + 
                 bp_risk_score * 0.25 + 
                 glucose_risk_score * 0.25 + 
                 cholesterol_risk_score * 0.25 + 
                 sodium_risk_score * 0.10), 2
            ) >= 1.5 then 'Priority Care'
            when round(
                (bmi_risk_score * 0.15 + 
                 bp_risk_score * 0.25 + 
                 glucose_risk_score * 0.25 + 
                 cholesterol_risk_score * 0.25 + 
                 sodium_risk_score * 0.10), 2
            ) >= 0.5 then 'Standard Care'
            else 'Routine Care'
        end as care_priority,
        
        -- Metadata
        current_timestamp as dbt_updated_at
        
    from risk_scoring
)

select * from final