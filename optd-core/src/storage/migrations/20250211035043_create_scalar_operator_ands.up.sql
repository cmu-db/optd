-- A scalar operator that takes in two scalar expressions and outputs a scalar expression that is the logical AND of the two expressions.
CREATE TABLE scalar_ands (
    -- The scalar expression id that this operator is associated with.
    scalar_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the expression.
    group_id BIGINT NOT NULL,
    -- The group id of left operand of the conjunction.
    left_group_id BIGINT NOT NULL,
    -- The group id of right operand of the conjunction.
    right_group_id BIGINT NOT NULL,

    FOREIGN KEY (scalar_expression_id) REFERENCES scalar_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES scalar_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (left_group_id) REFERENCES scalar_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE
    FOREIGN KEY (right_group_id) REFERENCES scalar_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on and's data fields.
CREATE UNIQUE INDEX scalar_ands_data_fields ON scalar_ands (left_group_id, right_group_id);

CREATE TRIGGER update_scalar_ands_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE scalar_ands SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
END;