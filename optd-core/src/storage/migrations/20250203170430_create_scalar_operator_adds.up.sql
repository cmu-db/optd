-- A scalar operator that adds two scalar expressions and returns their sum.
CREATE TABLE scalar_adds (
    -- The scalar expression id that this operator associated with.
    scalar_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the expression.
    group_id BIGINT NOT NULL,
    -- The group id of left operand of the addition.
    left_group_id BIGINT NOT NULL,
    -- The group id of right operand of the addition.
    right_group_id BIGINT NOT NULL,

    FOREIGN KEY (scalar_expression_id) REFERENCES scalar_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES relation_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (left_group_id) REFERENCES relation_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE
    FOREIGN KEY (right_group_id) REFERENCES relation_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on add's data fields.
CREATE UNIQUE INDEX scalar_adds_data_fields ON scalar_adds (left_group_id, right_group_id);

CREATE TRIGGER update_scalar_adds_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE OR REPLACE scalar_adds SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
END;
