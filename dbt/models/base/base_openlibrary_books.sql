SELECT *
From {{ source("openlibrary", "books") }} 