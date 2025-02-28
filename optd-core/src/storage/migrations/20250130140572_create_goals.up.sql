CREATE TABLE goals (
    -- The unique identifier of the relation group optimization goal.
    id INTEGER NOT NULL PRIMARY KEY,
    representative_goal_id BIGINT NOT NULL,
    -- The relation group id that this winner is associated with.
    group_id BIGINT NOT NULL,
    -- The required physical properties of the group.
    required_physical_properties JSON NOT NULL,
    -- The optimization status of the goal.
    -- It can be one of the following values: Unoptimized, Pending, Optimized.
    optimization_status TEXT NOT NULL,
    -- The time at which the group winner is created.
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
    
    FOREIGN KEY (representative_goal_id) REFERENCES goals (id) 
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES relation_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE
);


CREATE UNIQUE INDEX goals_unique_fields ON goals (group_id, required_physical_properties);


CREATE TRIGGER update_goals_id_relation_group_ids
AFTER UPDATE OF representative_group_id ON relation_groups
BEGIN
    UPDATE OR REPLACE goals SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
END;

