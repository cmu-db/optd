CREATE TABLE scalar_expressions (
    -- A unique identifier for a scalar expression in the optimizer.
    id INTEGER NOT NULL PRIMARY KEY,
    -- The representative group that a scalar expression belongs to.
    group_id BIGINT NOT NULL ON CONFLICT REPLACE,
    -- The kind of the scalar operator.
    operator_kind TEXT NOT NULL,
    -- The exploration status of a scalar expression.
    -- It can be one of the following values: Unexplored, Exploring, Explored.
    exploration_status INTEGER NOT NULL,
    -- The time at which the scalar expression is created.
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
    -- When group merging happens, the group id of the scalar expression is also updated.
    FOREIGN KEY (group_id) REFERENCES scalar_groups (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TRIGGER update_scalar_expressions_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE scalar_expressions SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
END;

