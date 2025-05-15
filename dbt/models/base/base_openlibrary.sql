-- ------------------------------------------------------------------------------
-- Model: Base_Beverage_openlibrary
-- Description: Unioning Tables with Books seperated by Search and Filter term
-- from the OpenLibrary API, the point of this is to test using both DLT State
-- & DuckDb connection to check for New API data (via DLT State) and deleted or
-- truncation data loss with DuckDB sql comparison with DLT State.
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-15 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------


SELECT search_term, topic_filter, title, author_name, 
publish_year, edition_count, key, subject_str
FROM {{ source("openlibrary", "sql_books") }} 
union all
SELECT search_term, topic_filter, title, author_name, 
publish_year, edition_count, key, subject_str
FROM {{ source("openlibrary", "python_books") }} 
union all
SELECT search_term, topic_filter, title, author_name, 
publish_year, edition_count, key, subject_str
FROM {{ source("openlibrary", "data_warehousing_books") }} 
union all
SELECT search_term, topic_filter, title, author_name, 
publish_year, edition_count, key, subject_str
FROM {{ source("openlibrary", "data_engineering_books") }} 
union all
SELECT search_term, topic_filter, title, author_name, 
publish_year, edition_count, key, subject_str
FROM {{ source("openlibrary", "apache_airflow_books") }} 
