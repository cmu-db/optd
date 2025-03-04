-- A logical scan operator that reads from a base table.
CREATE TABLE scans (
    -- The logical expression id that this scan is associated with.
    logical_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the scan.
    group_id BIGINT NOT NULL,
    -- The name of the table.
    -- TODO(yuchen): changes this to table id.
    table_name JSON NOT NULL,
    -- An optional filter expression for predicate pushdown into scan operators.
    predicate_group_id BIGINT NOT NULL,
    FOREIGN KEY (logical_expression_id) REFERENCES logical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES relation_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (predicate_group_id) REFERENCES scalar_groups (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on scan's data fields.
CREATE UNIQUE INDEX scans_data_fields ON scans (table_name, predicate_group_id);

CREATE TRIGGER update_scans_relation_group_ids
AFTER UPDATE OF representative_group_id ON relation_groups
BEGIN
    UPDATE OR REPLACE scans SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
END;

CREATE TRIGGER update_scans_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE scans SET predicate_group_id = NEW.representative_group_id WHERE predicate_group_id = OLD.representative_group_id;
END;