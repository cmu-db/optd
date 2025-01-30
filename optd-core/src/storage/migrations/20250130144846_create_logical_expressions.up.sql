CREATE TABLE logical_expressions (
    -- A unique identifier for a logical expression in the optimizer.
    id INTEGER NOT NULL PRIMARY KEY,
    -- The representative group that a logical expression belongs to.
    group_id BIGINT NOT NULL,
    -- The kind of the logical operator.
    operator_kind TEXT NOT NULL,
    -- The exploration status of a logical expression.
    -- It can be one of the following values: Unexplored, Exploring, Explored.
    exploration_status INTEGER NOT NULL,
    -- The time at which the logical expression is created.
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
    -- When group merging happens, the group id of the logical expression is also updated.
    FOREIGN KEY (group_id) REFERENCES relation_groups (representative_group_id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);
