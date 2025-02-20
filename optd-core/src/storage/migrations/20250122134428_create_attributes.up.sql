CREATE TABLE attributes (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The table that the attribute belongs to.
    table_id BIGINT NOT NULL,
    -- The name of the attribute.
    name TEXT NOT NULL,
    -- The kind of the attribute, integer, varchar, etc.
    kind TEXT NOT NULL,
    -- The index of the attribute in the table 
    -- e.g. For t1(v1 INTEGER, v2 TEXT), v1 has base_attribute_number 0 
    -- and v2 has base_attribute_number 1.
    base_attribute_number BIGINT NOT NULL,
    FOREIGN KEY (table_id) REFERENCES table_metadata(id) ON DELETE CASCADE
);
