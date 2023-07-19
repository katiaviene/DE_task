# EL pipeline for BikeShop db

Extraxtion of data from BikeShop database tables, data quality checks, loading tables to Excel files by tables' names.

## Running image

Set credentials:

DB_USERNAME

DB_PASSWORD

docker run -d -e DB_USERNAME=my_username -e DB_PASSWORD=my_password de_taks_barbora


## Retrieve excel files

docker run -d -v /path/on/host:/barbora_DE_task/copied_data de_taks_barbora

## Retrieve reports & sqlite db file

docker run -d -v /path/on/host:/barbora_DE_task de_taks_barbora
