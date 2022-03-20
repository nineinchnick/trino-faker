FROM trinodb/trino:374

ARG VERSION

ADD target/trino-faker-$VERSION/ /usr/lib/trino/plugin/faker/
ADD catalog/faker.properties /etc/trino/catalog/faker.properties
