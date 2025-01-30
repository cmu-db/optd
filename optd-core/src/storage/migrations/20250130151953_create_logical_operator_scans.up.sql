-- A logical scan operator that reads from a base table.
CREATE TABLE scans (
    -- The logical expression id that this scan is associated with.
    logical_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The name of the table.
    -- TODO(yuchen): changes this to table id.
    table_name TEXT NOT NULL,
    -- An optional filter expression for predicate pushdown into scan operators.
    predicate_group_id BIGINT NOT NULL,
    FOREIGN KEY (logical_expression_id) REFERENCES logical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (predicate_group_id) REFERENCES scalar_groups (representative_group_id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);
