CREATE KEYSPACE IF NOT EXISTS lightstream WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use lightstream;

CREATE TABLE topic_metadata (
    topic_name text,
    low_watermark counter,
    high_watermark counter,
    PRIMARY KEY (topic_name)
);


CREATE TABLE consumer_group (
    consumer_group_name text,
    topic_name text,
    offset counter,
    PRIMARY KEY (consumer_group_name, topic_name)
);