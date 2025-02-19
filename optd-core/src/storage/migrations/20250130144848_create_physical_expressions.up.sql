CREATE TABLE physical_expressions (
    -- A unique identifier for a physical expression in the optimizer.
    id INTEGER NOT NULL PRIMARY KEY,
    -- The representative group that a physical expression belongs to.
    goal_id BIGINT NOT NULL ON CONFLICT REPLACE,
    -- The kind of the physical operator.
    operator_kind TEXT NOT NULL,
    -- The time at which the physical expression is created.
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
    -- When group merging happens, the group id of the physical expression is also updated.
    FOREIGN KEY (goal_id) REFERENCES goals (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TRIGGER update_physical_expressions_goal_ids
AFTER UPDATE OF representative_goal_id ON goals
BEGIN
    UPDATE OR REPLACE physical_expressions SET goal_id = NEW.representative_goal_id WHERE goal_id = OLD.representative_goal_id;
END;
