-- A logical join operator combines rows from two relations.
CREATE TABLE nested_loop_joins (
    -- The logical expression id that this scan is associated with.
    logical_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the join.
    group_id BIGINT NOT NULL,
    -- The type of the join.
    join_type JSON NOT NULL,
    -- The outer input relation.
    outer_group_id BIGINT NOT NULL,
    -- The inner input relation.
    inner_group_id BIGINT NOT NULL,
    -- The join condition. e.g. `outer_column_a = inner_column_b`.
    condition_group_id BIGINT NOT NULL,

    FOREIGN KEY (logical_expression_id) REFERENCES logical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES relation_groups (id),
    FOREIGN KEY (outer_group_id) REFERENCES relation_groups (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
    FOREIGN KEY (inner_group_id) REFERENCES relation_groups (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
    FOREIGN KEY (condition_group_id) REFERENCES scalar_groups (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on join's data fields.
CREATE UNIQUE INDEX nested_loop_joins_data_fields ON nested_loop_joins (join_type, outer_group_id, inner_group_id, condition_group_id);

CREATE TRIGGER update_nested_loop_joins_relation_group_ids
AFTER UPDATE OF representative_group_id ON relation_groups
BEGIN
    UPDATE OR REPLACE nested_loop_joins SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
    UPDATE OR REPLACE nested_loop_joins SET outer_group_id = NEW.representative_group_id WHERE outer_group_id = OLD.representative_group_id;
    UPDATE OR REPLACE nested_loop_joins SET inner_group_id = NEW.representative_group_id WHERE inner_group_id = OLD.representative_group_id;
END;


CREATE TRIGGER update_nested_loop_joins_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE nested_loop_joins SET condition_group_id = NEW.representative_group_id WHERE condition_group_id = OLD.representative_group_id;
END;
