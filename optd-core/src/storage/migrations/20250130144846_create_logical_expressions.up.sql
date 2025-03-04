CREATE TABLE logical_expressions (
    -- A unique identifier for a logical expression in the optimizer.
    id INTEGER NOT NULL PRIMARY KEY,
    -- The representative group that a logical expression belongs to.
    group_id BIGINT NOT NULL ON CONFLICT REPLACE,
    -- The kind of the logical operator.
    operator_kind TEXT NOT NULL,
    -- The exploration status of a logical expression.
    -- It can be one of the following values: Unexplored, Exploring, Explored.
    exploration_status TEXT NOT NULL,
    -- The time at which the logical expression is created.
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
    -- When group merging happens, the group id of the logical expression is also updated.
    FOREIGN KEY (group_id) REFERENCES relation_groups (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TRIGGER update_logical_expressions_relation_group_ids
AFTER UPDATE OF representative_group_id ON relation_groups
BEGIN
    UPDATE OR REPLACE logical_expressions SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
END;
