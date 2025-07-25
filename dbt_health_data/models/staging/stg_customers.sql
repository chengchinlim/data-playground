{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw', 'customers') }}
),

transformed as (
    select
        -- Primary key
        customer_id,
        
        -- Customer details
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(trim(email)) as email,
        date_of_birth,
        gender,
        
        -- Calculated fields
        date_diff('year', date_of_birth, current_date) as age_years,
        case 
            when date_diff('year', date_of_birth, current_date) < 18 then 'Minor'
            when date_diff('year', date_of_birth, current_date) between 18 and 34 then 'Young Adult'
            when date_diff('year', date_of_birth, current_date) between 35 and 54 then 'Middle Age'
            when date_diff('year', date_of_birth, current_date) between 55 and 64 then 'Pre-Senior'
            when date_diff('year', date_of_birth, current_date) >= 65 then 'Senior'
            else 'Unknown'
        end as age_group,
        
        -- Metadata
        created_at,
        updated_at,
        current_timestamp as dbt_updated_at
        
    from source_data
)

select * from transformed