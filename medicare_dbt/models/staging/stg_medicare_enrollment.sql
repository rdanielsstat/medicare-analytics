with source as (
    select * from {{ source('medicare', 'medicare_monthly_enrollment') }}
),

cleaned as (
    select
        cast(year as integer)       as year,
        month,
        bene_geo_lvl,
        bene_state_abrvtn           as state,
        bene_state_desc             as state_name,
        bene_county_desc            as county,
        bene_fips_cd                as fips_code,
        tot_benes                   as total_beneficiaries,
        orgnl_mdcr_benes            as original_medicare_benes,
        ma_and_oth_benes            as medicare_advantage_benes,
        aged_tot_benes              as aged_beneficiaries,
        dsbld_tot_benes             as disabled_beneficiaries,
        male_tot_benes              as male_beneficiaries,
        female_tot_benes            as female_beneficiaries
    from source
    where month != 'Year'
      and year != 'Year'
)

select * from cleaned