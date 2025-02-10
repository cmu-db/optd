CREATE TABLE query_instances (
    -- A unique identifier for a query instance in the optimizer.
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The SQL query string that is being executed.
    query TEXT NOT NULL
);
