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
        female_beneficiaries
    from staging
    where bene_geo_lvl = 'State'
    and month = 'Year'
)
select * from by_state
order by year, state