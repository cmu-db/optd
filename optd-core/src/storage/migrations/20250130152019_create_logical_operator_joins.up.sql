-- A logical join operator combines rows from two relations.
CREATE TABLE joins (
    -- The logical expression id that this scan is associated with.
    logical_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the join.
    group_id BIGINT NOT NULL,
    -- The type of the join.
    join_type TEXT NOT NULL,
    -- The left input relation.
    left_group_id BIGINT NOT NULL,
    -- The right input relation.
    right_group_id BIGINT NOT NULL,
    -- The join condition. e.g. `left_column_a = right_column_b`.
    condition_group_id BIGINT NOT NULL,

    FOREIGN KEY (logical_expression_id) REFERENCES logical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES relation_groups (representative_group_id),
    FOREIGN KEY (left_group_id) REFERENCES relation_groups (representative_group_id) 
    ON UPDATE CASCADE ON DELETE CASCADE
    FOREIGN KEY (right_group_id) REFERENCES relation_groups (representative_group_id) 
    ON UPDATE CASCADE ON DELETE CASCADE
    FOREIGN KEY (condition_group_id) REFERENCES scalar_groups (representative_group_id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on join's data fields.
CREATE UNIQUE INDEX joins_data_fields ON joins (join_type, left_group_id, right_group_id, condition_group_id);
