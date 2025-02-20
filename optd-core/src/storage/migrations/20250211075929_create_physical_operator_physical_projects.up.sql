-- A physical project operator takes in a relation and outputs a relation with tuples that 
-- contain only specified attributes.
CREATE TABLE physical_projects (
    -- The physical expression id that this project is associated with.
    physical_expression_id INTEGER NOT NULL PRIMARY KEY,
     -- The group id of the project.
    group_id BIGINT NOT NULL,
    -- The input relation.
    child_group_id BIGINT NOT NULL,
    -- The projection list. A vector of scalar group ids, 
    fields_group_ids JSON NOT NULL,

    FOREIGN KEY (physical_expression_id) REFERENCES physical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (child_group_id) REFERENCES relation_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE
    -- (Not enforced)
    -- FOREIGN KEY json_each(fields_group_ids) REFERENCES scalar_groups (id)
    -- ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on project's data fields.
CREATE UNIQUE INDEX physical_projects_data_fields ON physical_projects (child_group_id, fields_group_ids);

CREATE TRIGGER update_physical_projects_relation_group_ids
AFTER UPDATE OF representative_group_id ON relation_groups
BEGIN
    UPDATE OR REPLACE physical_projects SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
    UPDATE OR REPLACE physical_projects SET child_group_id = NEW.representative_group_id WHERE child_group_id = OLD.representative_group_id;
END;

-- Approach 1:
CREATE TRIGGER update_physical_projects_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE physical_projects SET fields_group_ids = (
        SELECT json_group_array(
            CASE
                WHEN value = OLD.representative_group_id THEN NEW.representative_group_id
                ELSE value
            END
        ) FROM json_each(fields_group_ids) 
    );
END;
