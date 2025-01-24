-- The relational logical expressions table specifies 
-- which group a logical expression belongs to.
CREATE TABLE logical_exprs (
    -- The logical expression id.
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The logical operator descriptor id.
    logical_op_kind_id BIGINT NOT NULL,
    -- The group this logical expression belongs to.
    group_id BIGINT NOT NULL, -- groups.id
    -- Time at which the logical expression is created.
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
    FOREIGN KEY (logical_op_kind_id) REFERENCES logical_op_kinds(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (group_id) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);
