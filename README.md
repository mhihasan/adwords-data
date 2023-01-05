# full-text-search-postgres

## Run postgres database
1. Run `docker-compose -f docker-compose-pg.yml up -d` to start the database

## Altering system configuration
1. Tune system configuration for database using tools like [pgtune](https://pgtune.leopard.in.ua/#/).
2. Run `queries/alter_config.sql` to alter system configuration.
3. Restart database.

## Creating table
1. Run `01.create_db.sql` to create table.

## Insert data from csv
1. Run command to insert data from csv in postgres:
```sql
copy table_name from 'seed_data/test_keywords.csv' with (format csv, header true, delimiter ',');
```

## Run queries
1. Run `adwords_term_fetcher.py` to search adwords terms from database.
