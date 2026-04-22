with joins as (

    select
        employee_id,
        employee_name,
        city,
        address,
        job_title,
        annual_salary,
        hire_date
    from {{ ref('base_google_drive_hr_joins') }}

),

quits as (

    select
        employee_id,
        max(quit_date) as quit_date
    from {{ ref('base_google_drive_hr_quits') }}
    group by employee_id

),

joined as (

    select
        j.employee_id,
        j.employee_name,
        j.city,
        j.address,
        j.job_title,
        j.annual_salary,
        j.hire_date,
        q.quit_date
    from joins j
    left join quits q
        on j.employee_id = q.employee_id

)

select
    employee_id,
    employee_name,
    city,
    address,
    job_title,
    annual_salary,
    hire_date,
    quit_date as employment_end_date,

    case
        when quit_date is null then 1
        else 0
    end as is_active,

    case
        when quit_date is null then 'active'
        else 'terminated'
    end as employment_status,

    case
        when annual_salary is not null then annual_salary / 365.0
        else null
    end as daily_salary_cost

from joined