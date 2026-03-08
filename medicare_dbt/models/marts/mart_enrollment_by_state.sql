with staging as (
    select * from {{ ref('stg_medicare_enrollment') }}
),
by_state as (
    select
        year,
        state,
        state_name,
        fips_code,
        total_beneficiaries,
        original_medicare_benes,
        medicare_advantage_benes,
        aged_beneficiaries,
        disabled_beneficiaries,
        male_beneficiaries,
        female_beneficiaries,
        ROUND(medicare_advantage_benes / NULLIF(total_beneficiaries, 0) * 100, 1) AS ma_penetration_rate,
        ROUND(original_medicare_benes / NULLIF(total_beneficiaries, 0) * 100, 1)  AS ffs_penetration_rate        
    from staging
    where bene_geo_lvl = 'State'
    and month = 'Year'
)
select * from by_state
order by year, state