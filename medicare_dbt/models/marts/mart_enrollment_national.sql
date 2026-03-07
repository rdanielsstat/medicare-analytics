with staging as (
    select * from {{ ref('stg_medicare_enrollment') }}
),

national as (
    select
        {% if target.type == 'redshift' %}
        dateadd(month,
            case month
                when 'January'   then 0
                when 'February'  then 1
                when 'March'     then 2
                when 'April'     then 3
                when 'May'       then 4
                when 'June'      then 5
                when 'July'      then 6
                when 'August'    then 7
                when 'September' then 8
                when 'October'   then 9
                when 'November'  then 10
                when 'December'  then 11
            end,
            date_from_parts(year, 1, 1)
        ) as report_date,
        {% else %}
        make_date(year, to_number(to_char(to_date(month, 'Month'), 'MM'), '99')::integer, 1) as report_date,
        {% endif %}
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