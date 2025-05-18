





with validation_errors as (

    select
        weather_date, city
    from "camondagster"."public_base"."base_weather"
    group by weather_date, city
    having count(*) > 1

)

select *
from validation_errors


