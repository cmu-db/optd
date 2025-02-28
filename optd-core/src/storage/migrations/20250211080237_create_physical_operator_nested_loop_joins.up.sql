-- A physical join operator combines rows from two relations.
CREATE TABLE nested_loop_joins (
    -- The physical expression id that this scan is associated with.
    physical_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The goal id of the join.
    goal_id BIGINT NOT NULL,
    -- The type of the join.
    join_type JSON NOT NULL,
    -- The outer input relation.
    outer_goal_id BIGINT NOT NULL,
    -- The inner input relation.
    inner_goal_id BIGINT NOT NULL,
    -- The join condition. e.g. `outer_column_a = inner_column_b`.
    condition_group_id BIGINT NOT NULL,

    FOREIGN KEY (physical_expression_id) REFERENCES physical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (goal_id) REFERENCES goals (id),
    FOREIGN KEY (outer_goal_id) REFERENCES goals (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
    FOREIGN KEY (inner_goal_id) REFERENCES goals (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
    FOREIGN KEY (condition_group_id) REFERENCES scalar_groups (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on join's data fields.
CREATE UNIQUE INDEX nested_loop_joins_data_fields ON nested_loop_joins (join_type, outer_goal_id, inner_goal_id, condition_group_id);

CREATE TRIGGER update_nested_loop_joins_goal_ids
AFTER UPDATE OF representative_goal_id ON goals
BEGIN
    UPDATE OR REPLACE nested_loop_joins SET goal_id = NEW.representative_goal_id WHERE goal_id = OLD.representative_goal_id;
    UPDATE OR REPLACE nested_loop_joins SET outer_goal_id = NEW.representative_goal_id WHERE outer_goal_id = OLD.representative_goal_id;
    UPDATE OR REPLACE nested_loop_joins SET inner_goal_id = NEW.representative_goal_id WHERE inner_goal_id = OLD.representative_goal_id;
END;


CREATE TRIGGER update_nested_loop_joins_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE nested_loop_joins SET condition_group_id = NEW.representative_group_id WHERE condition_group_id = OLD.representative_group_id;
END;
