-- A scalar operator that represents a reference to a column.
CREATE TABLE scalar_column_refs (
    -- The scalar expression id that this column reference is associated with.
    scalar_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the expression.
    group_id BIGINT NOT NULL,
    -- The value of the column reference.
    column_index JSON NOT NULL,

    FOREIGN KEY (scalar_expression_id) REFERENCES scalar_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES scalar_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on constant's data fields.
CREATE UNIQUE INDEX scalar_column_refs_data_fields ON scalar_column_refs (column_index);

CREATE TRIGGER update_scalar_column_refs_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE scalar_column_refs SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
END;
