# EL pipeline for BikeShop db

Extraction of data from BikeShop database tables, data quality checks, loading tables to Excel files by tables' names.

## Running image

## Retrieve excel files

docker run -d -v /path/on/host:/barbora_DE_task/copied_data de_taks_barbora

## Retrieve reports & sqlite db file

docker run -d -v /path/on/host:/barbora_DE_task de_taks_barbora


Pipeline result: 

files: copied_data

db: copydb.db

Report with errors

result[data].txt
