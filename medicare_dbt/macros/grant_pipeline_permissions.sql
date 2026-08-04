{% macro grant_pipeline_permissions() %}
  {% set role_user = 'IAMR:medicare-analytics-ec2-airflow-role' %}

  {% if execute and target.type == 'redshift' %}
    {# Create the IAM user only if absent. Redshift lacks CREATE USER IF NOT EXISTS. #}
    {% set check_user %}
      SELECT 1 FROM pg_user WHERE usename = '{{ role_user }}'
    {% endset %}
    {% set results = run_query(check_user) %}

    {% if results | length == 0 %}
      CREATE USER "{{ role_user }}" WITH PASSWORD DISABLE;
    {% endif %}

    GRANT ALL ON DATABASE medicare_db TO "{{ role_user }}";
    GRANT ALL ON SCHEMA public TO "{{ role_user }}";
    CREATE SCHEMA IF NOT EXISTS dbt_medicare;
    GRANT ALL ON SCHEMA dbt_medicare TO "{{ role_user }}";
    ALTER SCHEMA dbt_medicare OWNER TO "{{ role_user }}";

    GRANT SELECT ON ALL TABLES IN SCHEMA dbt_medicare
      TO "IAMR:medicare-analytics-ec2-airflow-role";

    ALTER DEFAULT PRIVILEGES FOR USER medicare_admin IN SCHEMA dbt_medicare
      GRANT SELECT ON TABLES TO "IAMR:medicare-analytics-ec2-airflow-role";
  {% endif %}
{% endmacro %}