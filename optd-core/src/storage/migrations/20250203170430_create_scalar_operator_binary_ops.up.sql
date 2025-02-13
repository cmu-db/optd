-- A scalar operator that perform a binary operation on two scalar expressions.
CREATE TABLE scalar_binary_ops (
    -- The scalar expression id that this operator associated with.
    scalar_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the expression.
    group_id BIGINT NOT NULL,
    -- The kind of binary operation (e.g. +, -, *, /, ==, >).
    kind JSON NOT NULL,
    -- The group id of left operand of the binary operation.
    left_group_id BIGINT NOT NULL,
    -- The group id of right operand of the binary operation.
    right_group_id BIGINT NOT NULL,

    FOREIGN KEY (scalar_expression_id) REFERENCES scalar_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES scalar_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (left_group_id) REFERENCES scalar_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (right_group_id) REFERENCES scalar_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on binary operation's data fields.
CREATE UNIQUE INDEX scalar_binary_ops_data_fields ON scalar_binary_ops (kind, left_group_id, right_group_id);

CREATE TRIGGER update_scalar_binary_ops_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE scalar_binary_ops SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
    UPDATE OR REPLACE scalar_binary_ops SET left_group_id = NEW.representative_group_id WHERE left_group_id = OLD.representative_group_id;
    UPDATE OR REPLACE scalar_binary_ops SET right_group_id = NEW.representative_group_id WHERE right_group_id = OLD.representative_group_id;
END;
