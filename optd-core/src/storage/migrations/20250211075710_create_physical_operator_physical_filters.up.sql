-- A physical filter operator that selects rows matching a condition.
CREATE TABLE physical_filters (
    -- The physical expression id that this scan is associated with.
    physical_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the filter.
    group_id BIGINT NOT NULL,
    -- The input relation.
    child_group_id BIGINT NOT NULL,
    -- The predicate applied to the child relation: e.g. `column_a > 5`.
    predicate_group_id BIGINT NOT NULL,

    FOREIGN KEY (physical_expression_id) REFERENCES physical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES relation_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (child_group_id) REFERENCES relation_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (predicate_group_id) REFERENCES scalar_groups (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on filter's data fields.
CREATE UNIQUE INDEX physical_filters_data_fields ON physical_filters (child_group_id, predicate_group_id);

CREATE TRIGGER update_physical_filters_relation_group_ids
AFTER UPDATE OF representative_group_id ON relation_groups
BEGIN
    UPDATE OR REPLACE physical_filters SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
    UPDATE OR REPLACE physical_filters SET child_group_id = NEW.representative_group_id WHERE child_group_id = OLD.representative_group_id;
END;


CREATE TRIGGER update_physical_filters_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE physical_filters SET predicate_group_id = NEW.representative_group_id WHERE predicate_group_id = OLD.representative_group_id;
END;
