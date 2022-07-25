Trino Connector
===============

[![Build Status](https://github.com/nineinchnick/trino-faker/actions/workflows/release.yaml/badge.svg)](https://github.com/nineinchnick/trino-faker/actions/workflows/release.yaml)

This is a [Trino](http://trino.io/) connector that generates random data. It has two goals:
1. Be easy to use.
2. Support most Trino's data types.
3. Generate random data that looks as real as possible and is correct, that is it matches all the constraints.

# Quick Start

To run a Docker container with the connector, run the following:
```bash
docker run \
  -d \
  --name trino-faker \
  -p 8080:8080 \
  nineinchnick/trino-faker:0.13
```

Then use your favourite SQL client to connect to Trino running at http://localhost:8080

Try creating a table that looks like an existing table in a real database and insert some random data back into it:

```sql
CREATE TABLE faker.default.customer (LIKE production.public.customer EXCLUDING PROPERTIES);
INSERT INTO production.public.customers
SELECT *
FROM faker.default.customers
WHERE 1=1
AND born_at BETWEEN CURRENT_DATE - INTERVAL '150' YEAR AND CURRENT_DATE
AND age_years BETWEEN 0 AND 150
LIMIT 100;
```

To generate more realisting data, choose specific generators by setting the `generator` property on columns:
```sql
SHOW CREATE TABLE production.public.customers;
-- copy the output of the above query and add some properties:
CREATE TABLE faker.default.customer (
  id UUID NOT NULL,
  name VARCHAR NOT NULL WITH (generator = '#{Name.first_name} #{Name.last_name}'), 
  address VARCHAR NOT NULL WITH (generator = '#{Address.fullAddress}'),
  born_at DATE,
  age_years INTEGER
);
```

See the Datafaker's documentation for more information about
[the expression](https://www.datafaker.net/documentation/expressions/) syntax
and [available providers](https://www.datafaker.net/documentation/providers/).

# Usage

Download one of the ZIP packages, unzip it and copy the `trino-faker-0.11` directory to the plugin directory on every node in your Trino cluster.
Create a `faker.properties` file in your Trino catalog directory and set all the required properties.

```
connector.name=faker
```

After reloading Trino, you should be able to connect to the `faker` catalog.

## Generators

Particular data generator is selected based on the column type.

For `CHAR`, `VARCHAR` and `VARBINARY` column, the default generator uses the `Lorem ipsum` placeholder text.
Unbounded columns will have a random sentence with 3 to 40 words.

To have more control over the format of the generated data, use the `generator` column property. Some examples of valid generator expressions:
* `#{regexify '(a|b){2,3}'}`
* `#{regexify '\\.\\*\\?\\+'}`
* `#{bothify '????','false'}`
* `#{Name.first_name} #{Name.first_name} #{Name.last_name}`
* `#{number.number_between '1','10'}`

Generator expressions cannot be used for non-character based columns. To limit their data range, specify constraints in the `WHERE` clause.

## Number of generated rows

To control how many rows are generated for a table, use the `LIMIT` clause in the query.
A default limit can be set using the `default_limit` table or schema property or in the connector configuration file.

## Null values

For columns without the `NOT NULL` constraint, null values will be generated using the default probability of 50% (0.5).
It can be modified using the `null_probability` property set for a column, table or schema.
The default value of 0.5 can be also modified in the connector configuration file.

# Build

Run all the unit test classes.
```
mvn test
```

Creates a deployable jar file
```
mvn clean compile package
```

Copy jar files in target directory to use git connector in your Trino cluster.
```
cp -p target/*.jar ${PLUGIN_DIRECTORY}/faker/
```

# Deploy

An example command to run the Trino server with the faker plugin and catalog enabled:

```bash
src=$(git rev-parse --show-toplevel)
docker run \
  -v $src/target/trino-faker-0.12-SNAPSHOT:/usr/lib/trino/plugin/faker \
  -v $src/catalog:/usr/lib/trino/default/etc/catalog \
  -p 8080:8080 \
  --name trino \
  -d \
  trinodb/trino:391
```

Connect to that server using:
```bash
docker run -it --rm --link trino trinodb/trino:391 trino --server trino:8080 --catalog faker --schema default
```
