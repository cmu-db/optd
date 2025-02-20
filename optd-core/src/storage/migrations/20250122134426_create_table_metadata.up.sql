CREATE TABLE table_metadata (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The name of the table,
    name TEXT NOT NULL,
    -- The namespace that the table belongs to.
    namespace_id BIGINT NOT NULL,
    -- The time at which the database was created.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    
    FOREIGN KEY (namespace_id) REFERENCES namespace_metadata(id) ON DELETE CASCADE
);
