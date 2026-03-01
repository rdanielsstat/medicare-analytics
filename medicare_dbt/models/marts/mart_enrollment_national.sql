with staging as (
    select * from {{ ref('stg_medicare_enrollment') }}
),

national as (
    select
        make_date(year, to_number(to_char(to_date(month, 'Month'), 'MM'), '99')::integer, 1) as report_date,
        total_beneficiaries,
        original_medicare_benes,
        medicare_advantage_benes,
        aged_beneficiaries,
        disabled_beneficiaries,
        male_beneficiaries,
        female_beneficiaries
    from staging
    where bene_geo_lvl = 'National'
)

select * from national
order by report_date