-- A logical filter operator that selects rows matching a condition.
CREATE TABLE filters (
    -- The logical expression id that this scan is associated with.
    logical_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the filter.
    group_id BIGINT NOT NULL,
    -- The input relation.
    child_group_id BIGINT NOT NULL,
    -- The predicate applied to the child relation: e.g. `column_a > 5`.
    predicate_group_id BIGINT NOT NULL,

    FOREIGN KEY (logical_expression_id) REFERENCES logical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES relation_groups (representative_group_id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (child_group_id) REFERENCES relation_groups (representative_group_id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (predicate_group_id) REFERENCES scalar_groups (representative_group_id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on join's data fields.
CREATE UNIQUE INDEX filters_data_fields ON filters (child_group_id, predicate_group_id);
