--Choisir la base de données et le schema
use database MC_DATABASE;
use schema DEMO;
use warehouse COMPUTE_WH;
--voir tables disponibles
show tables;
--Voir schéma table
DESCRIBE TABLE POKEDEX;
--Avoir un premier aperçu des tables :
SELECT * FROM POKEDEX LIMIT 10;

SELECT * FROM EVENTS LIMIT 10;
SELECT * FROM LIEU;
--Filtre
SELECT * FROM POKEDEX WHERE GENUS = 'Golem Ancien';

--Aggregation
SELECT  REGION, COUNT(DISTINCT NOM) as Nb,COUNT(*) as Nb2
FROM LIEU
GROUP BY REGION 
ORDER BY NB DESC ;

SELECT GENUS, AVG(TAILLE) as taille_moyenne, SUM(POIDS), COUNT(*) as nb_pok FROM POKEDEX
GROUP BY GENUS HAVING nb_pok > 1;

--time functions (equivalent datetime sur python)
    
 SELECT  TO_TIMESTAMP('2122380525'),    TO_DATE('2122380525') ;
SELECT  TO_TIMESTAMP('212238052'),    TO_DATE('212238052') ;
  
SELECT  TO_DATE(TO_CHAR(ROUND(10000000*Date))) as Date_format FROM EVENTS --ORDER BY Date_Format DESC

WITH events_until_2000 as (SELECT  ID, ID_POKEMON, TYPE, GAIN, ID_LIEU,TO_DATE(TO_CHAR(ROUND(10000000*Date))) as Date_format FROM EVENTS 
WHERE Date_format <= DATEADD(day, -10,'2000-01-10'))

SELECT Date_format,DATE_TRUNC(month,Date_format  ) FROM events_until_2000 LIMIT 10;

SELECT  DATEDIFF(day,Date_format ,'2023-02-09' ) as diff_day , DATEDIFF(year,Date_format ,'2023-02-09' ) as diff_year FROM events_until_2000;

--CASE WHEN THEN WHEN THEN ELSE END = IF ELIF ELSE END 
SELECT DISTINCT TYPE FROM EVENTS;

--marche pas :(
SELECT AVG(TYPE) FROM EVENTS 

SELECT ID_POKEMON, AVG(
    CASE WHEN TYPE='combat' THEN 0 
    ELSE  1 END ) AS BINARY_TYPE
FROM EVENTS
GROUP BY ID_POKEMON;


SELECT DATE_TRUNC(year,Date_format  ), AVG(
    CASE WHEN TYPE='combat' THEN 0 
    ELSE  1 END ) AS BINARY_TYPE
FROM events_until_2000
GROUP BY DATE_TRUNC(year,Date_format  );

--Une Première jointure (pour obtenir infos sur pokemon) avec une Common Table expression
SELECT * FROM events LIMIT 10;
SELECT * FROM elevage LIMIT 10;
SELECT * FROM pokedex LIMIT 10;
SELECT * FROM LIEU LIMIT 10;
WITH events_pokedex AS (
    SELECT  events.id as event_id, id_pokemon,type, gain, TO_DATE(TO_CHAR(ROUND(10000000*Date))) as Date, id_lieu,
    ESPECE, GENUS, TAIlle, POIDS
    FROM events
    INNER JOIN pokedex
    on events.id_pokemon = pokedex.id),
                
--Deuxieme jointure (infos sur lieu)

table_all  AS( SELECT  event_id, id_pokemon,type, gain, Date, id_lieu, ESPECE, GENUS, TAIllE, POIDS,
              NOM, REGION
FROM events_pokedex
JOIN LIEU
ON  events_pokedex.id_lieu = LIEU.id)

--SELECT * FROM table_all  LIMIT 10;

-- Usage de ROW NUMBER(), PARTITION BY OVER : Pagination/ ranking
-- AGG OVER( PARTITION BY ) indices agrégés par catégorie
SELECT event_id, id_pokemon,type, gain, Date, id_lieu, ESPECE, GENUS, TAIllE, POIDS,NOM, REGION,
        ROW_NUMBER() OVER(PARTITION BY REGION ORDER BY Date DESC ) AS "Row Number",
        SUM(GAIN) OVER(PARTITION BY REGION) AS PRICE_NATION,
        COUNT(EVENT_ID) OVER(PARTITION BY REGION) AS Nb_event_REGION,
        --Equivalent de cumsum() de pandas : 
        (SUM(GAIN) OVER(PARTITION BY REGION
       ORDER BY DATE DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS CumulativeTotal
        FROM table_all 
        ORDER BY region, "Row Number" LIMIT 1200;
 --utile pour selectionner sous echantillon de transactions tout en maximisant CA

-- difference group by vs partition by 
SELECT REGION , SUM(GAIN) as PRICE
FROM table_all 
GROUP BY REGION;
