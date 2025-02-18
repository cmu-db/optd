CREATE TABLE database_metadata (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The name of the database.
    name TEXT NOT NULL,
    -- The time at which the database was created.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
