-- A scalar operator that perform logic operation on its operands.
CREATE TABLE scalar_logic_ops (
    -- The scalar expression id that this project is associated with.
    scalar_expression_id INTEGER NOT NULL PRIMARY KEY,
     -- The group id of the project.
    group_id BIGINT NOT NULL,
    -- The kind of logic operation (e.g. AND, OR).
    kind JSON NOT NULL,
    -- The group id of the children operands.
    children_group_ids JSON NOT NULL,

    FOREIGN KEY (scalar_expression_id) REFERENCES scalar_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE
    -- (Not enforced)
    -- FOREIGN KEY json_each(children_group_ids) REFERENCES scalar_groups (id)
    -- ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on logic operation's data fields.
CREATE UNIQUE INDEX scalar_logic_ops_data_fields ON scalar_logic_ops (kind, children_group_ids);

CREATE TRIGGER update_scalar_logic_ops_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE scalar_logic_ops SET children_group_ids = (
        SELECT json_group_array(
            CASE
                WHEN value = OLD.representative_group_id THEN NEW.representative_group_id
                ELSE value
            END
        ) FROM json_each(children_group_ids) 
    );
END;
