-- A logical project operator takes in a relation and outputs a relation with tuples that 
-- contain only specified attributes.
CREATE TABLE projects (
    -- The logical expression id that this project is associated with.
    logical_expression_id INTEGER NOT NULL PRIMARY KEY,
     -- The group id of the project.
    group_id BIGINT NOT NULL,
    -- The input relation.
    child_group_id BIGINT NOT NULL,
    -- The projection list. A vector of scalar group ids, 
    fields_group_ids JSONB NOT NULL,

    FOREIGN KEY (logical_expression_id) REFERENCES logical_expressions (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (child_group_id) REFERENCES relation_groups (id)
    ON UPDATE CASCADE ON DELETE CASCADE
    -- (Not enforced)
    -- FOREIGN KEY json_each(fields_group_ids) REFERENCES scalar_groups (id)
    -- ON UPDATE CASCADE ON DELETE CASCADE
);

-- Unique index on project's data fields.
CREATE UNIQUE INDEX projects_data_fields ON projects (child_group_id, fields_group_ids);

CREATE TRIGGER update_projects_relation_group_ids
AFTER UPDATE OF representative_group_id ON relation_groups
BEGIN
    UPDATE OR REPLACE projects SET group_id = NEW.representative_group_id WHERE group_id = OLD.representative_group_id;
    UPDATE OR REPLACE projects SET child_group_id = NEW.representative_group_id WHERE child_group_id = OLD.representative_group_id;
END;

-- Approach 1:
CREATE TRIGGER update_projects_scalar_group_ids
AFTER UPDATE OF representative_group_id ON scalar_groups
BEGIN
    UPDATE projects SET fields_group_ids = (
        SELECT json_group_array(
            CASE
                WHEN value = OLD.representative_group_id THEN NEW.representative_group_id
                ELSE value
            END
        ) FROM json_each(fields_group_ids) 
    );
END;

-- Approach 2:
-- CREATE VIEW project_fields AS 
-- SELECT 
--     logical_expressions.id as logical_expression_id, 
--     json_each.value as field_group_id
-- FROM projects, json_each(fields_group_ids);


-- CREATE TRIGGER update_projects_scalar_group_ids
-- AFTER UPDATE OF representative_group_id ON scalar_groups
-- BEGIN
--     UPDATE project_fields SET field_group_id = NEW.representative_group_id WHERE field_group_id = OLD.representative_group_id;
-- END;

-- CREATE TRIGGER update_project_fields
-- INSTEAD OF UPDATE ON project_fields
-- BEGIN
--     UPDATE projects SET fields_group_ids = (
--         SELECT json_group_array(
--             CASE
--                 WHEN value = OLD.field_group_id THEN NEW.field_group_id
--                 ELSE value
--             END
--         ) FROM json_each(fields_group_ids) 
--     ) WHERE logical_expression_id = OLD.logical_expression_id;
-- END;
