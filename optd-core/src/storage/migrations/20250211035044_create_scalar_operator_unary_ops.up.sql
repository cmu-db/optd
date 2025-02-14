-- A scalar operator that perform a unary operation on its child operands.
CREATE TABLE scalar_unary_ops (
    -- The scalar expression id that this operator associated with.
    scalar_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the expression.
    group_id BIGINT NOT NULL,
    -- The kind of unary operation (e.g. +, -, *, /, ==, >).
    kind JSON NOT NULL,
    -- The group id of child operand of the  operation.
    child_group_id BIGINT NOT NULL,

    FOREIGN KEY (scalar_expression_id) REFERENCES scalar_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES scalar_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (child_group_id) REFERENCES scalar_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on unary operation's data fields.
CREATE UNIQUE INDEX scalar_unary_ops_data_fields ON scalar_unary_ops (kind, child_group_id);

CREATE TRIGGER update_scalar_unary_ops_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE scalar_unary_ops SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
    UPDATE OR REPLACE scalar_unary_ops SET child_group_id = NEW.representative_group_id WHERE child_group_id = OLD.representative_group_id;
END;

