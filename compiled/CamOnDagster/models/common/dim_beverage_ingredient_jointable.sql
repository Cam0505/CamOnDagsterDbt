SELECT distinct id_drink as Beverage_ID
    from "camondagster"."beverage_data"."ingredients_table" as it
    where id_drink is not null