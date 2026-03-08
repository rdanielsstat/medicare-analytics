with staging as (
    select * from {{ ref('stg_medicare_enrollment') }}
),
by_state as (
    select
        year,
        state,
        state_name,
        fips_code,
        sum(total_beneficiaries)        as total_beneficiaries,
        sum(original_medicare_benes)    as original_medicare_benes,
        sum(medicare_advantage_benes)   as medicare_advantage_benes,
        sum(aged_beneficiaries)         as aged_beneficiaries,
        sum(disabled_beneficiaries)     as disabled_beneficiaries,
        sum(male_beneficiaries)         as male_beneficiaries,
        sum(female_beneficiaries)       as female_beneficiaries
    from staging
    where bene_geo_lvl = 'State'
    and month != 'Year'
    group by year, state, state_name, fips_code
)
select * from by_state
order by year, state