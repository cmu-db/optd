CREATE TABLE namespace_metadata (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The namespace.
    name TEXT NOT NULL,
    -- The database that the namespace belongs to.
    database_id BIGINT NOT NULL,
    -- The time at which the database was created.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,

    FOREIGN KEY (database_id) REFERENCES database_metadata(id) ON DELETE CASCADE
);
