-- Table to replicate
CREATE TABLE table_to_replicate(
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  name TEXT NOT NULL,
  archived BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX idx_table_to_replicate_name ON table_to_replicate (name);

CREATE TABLE table_to_replicate2(
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  name TEXT NOT NULL,
  archived BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE table_to_replicate3(
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  name TEXT NOT NULL,
  archived BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO table_to_replicate (name, archived)
  VALUES ('test row 1.1', true),
  ('test row 1.2', false);

INSERT INTO table_to_replicate2 (name, archived)
  VALUES ('test row 2.1', true),
  ('test row 2.2', false);

INSERT INTO table_to_replicate3 (name, archived)
  VALUES ('test row 3.1', true),
  ('test row 3.2', false);
