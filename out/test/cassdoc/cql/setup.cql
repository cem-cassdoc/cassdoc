CREATE KEYSPACE IF NOT EXISTS integration_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

DROP TABLE IF EXISTS integration_test.logs;

CREATE TABLE IF NOT EXISTS integration_test.logs (
    id text,
    query text,
    PRIMARY KEY (id)
);

INSERT into integration_test.logs(id, query) values ('1','cinema');