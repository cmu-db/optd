-- A projection field list.
-- This is a private table only for the logical project operator to store 
-- the variable-length scalar childen.
CREATE TABLE _projection_fields (
    -- The id of the projection field list.
    id INTEGER NOT NULL PRIMARY KEY,
    -- THe index of the field in the projection list.
    location BIGINT NOT NULL,
    -- The field id.
    field_group_id BIGINT NOT NULL,

    FOREIGN KEY (field_group_id) REFERENCES scalar_groups (representative_group_id)
);

-- A logical project operator takes in a relation and outputs a relation with tuples that 
-- contain only specified attributes.
CREATE TABLE projects (
    -- The logical expression id that this scan is associated with.
    logical_expression_id INTEGER NOT NULL PRIMARY KEY,
    -- The input relation.
    child_group_id BIGINT NOT NULL,
    -- The projection list.
    fields_id BIGINT NOT NULL,

    FOREIGN KEY (logical_expression_id) REFERENCES logical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (child_group_id) REFERENCES relation_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (fields_id) REFERENCES _projection_fields (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);
