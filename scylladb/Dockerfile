FROM scylladb/scylla

# Add CQL queries to be run upon boot
# COPY ./scylladb/create-tables.cql /docker-entrypoint-initdb.d/create-tables.cql

# COPY ./scylladb/wrapper.sh /wrapper.sh

COPY create-tables.cql /docker-entrypoint-initdb.d/create-tables.cql

COPY wrapper.sh /wrapper.sh

ENTRYPOINT ["/wrapper.sh"]