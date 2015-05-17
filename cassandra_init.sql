CREATE KEYSPACE netflix WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE movies (
  id varchar PRIMARY KEY,
  saved_at varchar,
  title varchar,
  directors list<varchar>,
  year int,
  genres list<varchar>
);

CREATE TABLE series (
  id varchar PRIMARY KEY,
  saved_at varchar,
  title varchar,
  directors list<varchar>,
  year int,
  genres list<varchar>,
  seasons int
);
