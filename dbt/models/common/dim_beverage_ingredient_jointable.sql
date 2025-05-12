SELECT distinct id_drink as Beverage_ID
    from {{ source("beverages", "ingredients_table") }} as it
    where id_drink is not null