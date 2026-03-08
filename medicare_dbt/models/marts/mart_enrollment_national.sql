with staging as (
    select * from {{ ref('stg_medicare_enrollment') }}
),

national as (
    select 
        {% if target.type == 'redshift' %}
        TO_DATE(CAST(year AS VARCHAR) || '-' ||
            case month
                when 'January'   then '01'
                when 'February'  then '02'
                when 'March'     then '03'
                when 'April'     then '04'
                when 'May'       then '05'
                when 'June'      then '06'
                when 'July'      then '07'
                when 'August'    then '08'
                when 'September' then '09'
                when 'October'   then '10'
                when 'November'  then '11'
                when 'December'  then '12'
            end || '-01', 'YYYY-MM-DD') as report_date,
        {% else %}
        make_date(year, to_number(to_char(to_date(month, 'Month'), 'MM'), '99')::integer, 1) as report_date,
        {% endif %}
        total_beneficiaries,
        original_medicare_benes,
        medicare_advantage_benes,
        aged_beneficiaries,
        disabled_beneficiaries,
        male_beneficiaries,
        female_beneficiaries,
        ROUND(medicare_advantage_benes / NULLIF(total_beneficiaries, 0) * 100, 1) AS ma_penetration_rate,
        ROUND(original_medicare_benes / NULLIF(total_beneficiaries, 0) * 100, 1)  AS ffs_penetration_rate,
        year,
        month
      from staging
     where bene_geo_lvl = 'National'
       and month != 'Year'
)

select * from national
order by report_date