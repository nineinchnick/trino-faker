FROM trinodb/trino:375

ARG VERSION

ADD target/trino-faker-$VERSION/ /usr/lib/trino/plugin/faker/
ADD catalog/faker.properties /etc/trino/catalog/faker.properties
