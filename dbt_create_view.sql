{{ config(materialized='view', tags=['create_view']) }}

with table_all  AS( 
SELECT  events.id as event_id, id_pokemon, gain, TO_DATE(TO_CHAR(ROUND(10000000*Date))) as Date,
 id_lieu
FROM  {{ source('db_snowflake', 'Events') }} as events
JOIN  {{ source('db_snowflake', 'Pokedex') }}  as pokedex on events.id_pokemon = pokedex.id)

SELECT * FROM table_all  LIMIT 10

-- then open a terminal and run the command : dbt run -m tag:create_view
