-- A logical scan operator that reads from a base table.
CREATE TABLE scans (
    -- The logical expression id that this scan is associated with.
    logical_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the scan.
    group_id BIGINT NOT NULL,
    -- The name of the table.
    -- TODO(yuchen): changes this to table id.
    table_name TEXT NOT NULL,
    -- An optional filter expression for predicate pushdown into scan operators.
    predicate_group_id BIGINT NOT NULL,
    FOREIGN KEY (logical_expression_id) REFERENCES logical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES relation_groups (representative_group_id)
    FOREIGN KEY (predicate_group_id) REFERENCES scalar_groups (representative_group_id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on scan's data fields.
CREATE UNIQUE INDEX scans_data_fields ON scans (table_name, predicate_group_id);