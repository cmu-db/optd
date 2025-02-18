CREATE TABLE attributes (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The table that the attribute belongs to.
    table_id BIGINT NOT NULL,
    -- The name of the attribute.
    name TEXT NOT NULL,
    -- The kind of the attribute, integer, varchar, etc.
    kind TEXT NOT NULL,
    
    FOREIGN KEY (table_id) REFERENCES table_metadata(id) ON DELETE CASCADE
);
