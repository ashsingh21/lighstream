CREATE KEYSPACE IF NOT EXISTS lightstream WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use lightstream;

CREATE TABLE topic_metadata (
    topic_name varchar,
    high_watermark counter,
    low_watermark counter,
    PRIMARY KEY (topic_name)
);


CREATE TABLE consumer_group (
    consumer_group_name varchar,
    topic_name varchar,
    offset counter,
    PRIMARY KEY (consumer_group_name, topic_name)
);