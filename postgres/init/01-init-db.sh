#!/bin/bash
set -e
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE SCHEMA inventory AUTHORIZATION postgres;
  BEGIN;
    CREATE TABLE IF NOT EXISTS inventory.customers (
	  id serial4 NOT NULL,
	  first_name varchar(255) NULL,
	  last_name varchar(255) NULL,
	  email varchar(255) NULL,
	  CONSTRAINT customers_pk PRIMARY KEY (id)
    );
	INSERT INTO inventory.customers (first_name,last_name,email) VALUES
	  ('Sally','Thomas','sally.thomas@acme.com'),
	  ('George','Bailey','gbailey@foobar.com'),
	  ('Edward','Walker','ed@walker.com'),
	  ('Jonh','Kretchmar','annek@noanswer.org');
  COMMIT;
EOSQL