
{% set database_name = var('database_name', 'duckdb_stats') %}

WITH pre_aggregated_data AS (
    SELECT
        month_start_date :: date as download_date,
        name AS system_name,
        release AS system_release,
        version,
        project,
        country_code,
        cpu,
        CASE
            WHEN python IS NULL THEN NULL
            ELSE CONCAT(
                SPLIT_PART(python, '.', 1),
                '.',
                SPLIT_PART(python, '.', 2)
            )
        END AS python_version,
        monthly_download_sum
    FROM
        {{ 
            source('external_source','pypi_file_downloads')
        }}
    WHERE
        download_date >= '{{ var("start_date") }}'
        AND download_date < '{{ var("end_date") }}'
)

SELECT
    MD5(CONCAT_WS('|', download_date, system_name, system_release, version, project, country_code, cpu, python_version)) AS load_id,
    download_date,
    system_name,
    system_release,
    version,
    project,
    country_code,
    cpu,
    python_version,
    monthly_download_sum    
FROM
    pre_aggregated_data
GROUP BY
    ALL
