CREATE TABLE relation_group_goals (
    -- The unique identifier of the relation group optimization goal.
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The relation group id that this winner is associated with.
    relation_group_id BIGINT NOT NULL,
    -- The required physical properties of the group.
    required_physical_properties JSON,
    -- The physical expression id of the winner.
    -- TODO(yuchen): handle property enforcement.

    -- The optimization status of the goal.
    -- It can be one of the following values: Unoptimized, Pending, Optimized.
    optimization_status TEXT,
    winner_physical_expression_id BIGINT,
    -- The time at which the group winner is created.
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
    
    FOREIGN KEY (relation_group_id) REFERENCES relation_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (winner_physical_expression_id) REFERENCES physical_expressions (id)
);


CREATE UNIQUE INDEX relation_group_goals_unique_fields ON relation_group_goals (relation_group_id, required_physical_properties);
