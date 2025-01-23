-- A relational group contains a set of relational expressions 
-- that are logically equivalent.
CREATE TABLE rel_groups (
    -- The group identifier.
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- Time at which the group is created.
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL
);
