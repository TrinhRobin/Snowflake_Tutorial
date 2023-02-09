create database MC_DATABASE;
create schema demo;
create or replace  stage s3_data
  url = 's3://path/to/bucket'
  credentials = (aws_key_id='',
                aws_secret_key=''); 
                
                --['Locations', 'elevage', 'events', 'lieu', 'pokedex', 'sqlite
                
CREATE FILE FORMAT CLASSIC_CSV;
TYPE = 'csv'
COMPRESSION = 'AUTO' 
RECORD_DELIMITER = '\n'
FIELD_DELIMITER = ',' 
SKIP_HEADER = 1 
DATE_FORMAT = 'AUTO' 
TIMESTAMP_FORMAT = 'AUTO'
FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'
TRIM_SPACE = FALSE
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
ESCAPE = 'NONE' 
ESCAPE_UNENCLOSED_FIELD = '\134' 
NULL_IF = ('\\N');

list @s3_data;

create table Lieu( 
    id numeric primary key, 
    nom varchar, 
    region varchar
);

create table pokedex( 
   id numeric primary key,
    espece varchar,
    genus varchar,
    taille numeric,
     poids numeric
);

create or replace  table events( 
   id numeric primary key,
  id_pokemon numeric foreign key references pokedex(id) ,
    type varchar,
    gain float,
    date float,
    id_lieu numeric foreign key references lieu(id)
);
create table elevage( 
   id numeric primary key,
  prenom varchar,
  id_espece numeric foreign key references pokedex(id) ,
    id_parent numeric, 
    date_naissance numeric ,
    date_mort numeric,
    lieu_naissance numeric foreign key references lieu(id)
    
);
copy into Lieu
from @s3_data/pokemon_db/lieu.csv
file_format =  CLASSIC_CSV
on_error = "CONTINUE" ;


copy into EVENTS
from @s3_data/pokemon_db/events.csv
file_format =  CLASSIC_CSV
on_error = "CONTINUE" ;

copy into pokedex
from @s3_data/pokemon_db/pokedex.csv
file_format =  CLASSIC_CSV
on_error = "CONTINUE" ;


