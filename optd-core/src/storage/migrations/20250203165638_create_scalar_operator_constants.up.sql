-- A scalar operator that represents a constant.
CREATE TABLE scalar_constants (
    -- The scalar expression id that this constant is associated with.
    scalar_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the constant.
    group_id BIGINT NOT NULL,
    -- The value of the constant.
    value JSON NOT NULL,

    FOREIGN KEY (scalar_expression_id) REFERENCES scalar_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES scalar_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on constant's data fields.
CREATE UNIQUE INDEX scalar_constants_data_fields ON scalar_constants (value);

CREATE TRIGGER update_scalar_constants_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE scalar_constants SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
END;
