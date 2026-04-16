SELECT 
    _FILE,
    _LINE,
    _MODIFIED AS _MODIFIED_TS ,
    _fivetran_synced AS _fivetran_synced_TS,
    CAST(EMPLOYEE_ID AS STRING) AS EMPLOYEE_ID,
    try_to_date(replace(hire_date, 'day ', '')) as hire_date,
    NAME AS EMPLOYEE_NAME,
    CITY,
    ADDRESS,
    TITLE as JOB_TITLE,
    ANNUAL_SALARY
FROM {{ source('google_drive', 'hr_joins') }}