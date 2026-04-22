with date_spine as (
    select distinct
        date
    from {{ ref('int_daily_revenue') }}

),
expense_summary as (

    select
        expense_date as date,
        sum(coalesce(expense_amount, 0)) as other_expense_amount
    from {{ ref('base_google_drive_expenses') }}
    group by 1

),
employee_daily_cost as (

    select
        d.date,
        sum(coalesce(e.daily_salary_cost, 0)) as salary_cost_amount
    from date_spine d
    join {{ ref('int_employee') }} e
        on d.date >= e.hire_date
       and (
            e.employment_end_date is null
            or d.date <= e.employment_end_date
       )
    group by 1
)

select
    d.date,
    coalesce(x.other_expense_amount, 0) as other_expense_amount,
    coalesce(s.salary_cost_amount, 0) as salary_cost_amount,
    coalesce(x.other_expense_amount, 0) + coalesce(s.salary_cost_amount, 0) as total_cost_amount
from date_spine d
left join expense_summary x
    on d.date = x.date
left join employee_daily_cost s
    on d.date = s.date
order by d.date