-- A physical filter operator that selects rows matching a condition.
CREATE TABLE physical_filters (
    -- The physical expression id that this scan is associated with.
    physical_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The goal id of the filter.
    goal_id BIGINT NOT NULL,
    -- The input goal.
    child_goal_id BIGINT NOT NULL,
    -- The predicate applied to the child relation: e.g. `column_a > 5`.
    predicate_group_id BIGINT NOT NULL,

    FOREIGN KEY (physical_expression_id) REFERENCES physical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (goal_id) REFERENCES goals (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (child_goal_id) REFERENCES goals (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (predicate_group_id) REFERENCES scalar_groups (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on filter's data fields.
CREATE UNIQUE INDEX physical_filters_data_fields ON physical_filters (child_goal_id, predicate_group_id);

CREATE TRIGGER update_physical_filters_goal_ids
AFTER UPDATE OF representative_goal_id ON goals
BEGIN
    UPDATE OR REPLACE physical_filters SET goal_id = NEW.representative_goal_id WHERE goal_id = OLD.representative_goal_id;
    UPDATE OR REPLACE physical_filters SET child_goal_id = NEW.representative_goal_id WHERE child_goal_id = OLD.representative_goal_id;
END;


CREATE TRIGGER update_physical_filters_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE physical_filters SET predicate_group_id = NEW.representative_group_id WHERE predicate_group_id = OLD.representative_group_id;
END;
