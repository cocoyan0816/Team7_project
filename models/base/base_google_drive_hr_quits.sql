SELECT 
    _FILE,
    _LINE,
    _MODIFIED AS _MODIFIED_TS ,
    _fivetran_synced AS _fivetran_synced_TS,
    CAST(EMPLOYEE_ID AS STRING) AS EMPLOYEE_ID,
    QUIT_DATE
FROM {{ source('google_drive', 'hr_quits') }}