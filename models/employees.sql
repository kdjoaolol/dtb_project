with calc_employees as (
    select 

    datediff(year, birth_date, current_date) as age,
    datediff(year, hire_date, current_date) as lengthofservice,
    first_name || ' ' ||last_name as full_name, 
    *
    from {{ source('sources', 'employees')}}
)

select * from calc_employees