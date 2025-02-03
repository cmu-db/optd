CREATE TABLE scalar_groups (
    -- A unique identifier for a group of scalar expressions in the memo table.
    id INTEGER NOT NULL PRIMARY KEY,
    -- The representative group id of a scalar group. 
    -- At insertion time, the representative group id is the same as the id,
    -- but it can be updated later.
    representative_group_id BIGINT NOT NULL,
    -- The exploration status of a scalar group. 
    -- It can be one of the following values: Unexplored, Exploring, Explored.
    exploration_status INTEGER NOT NULL,
    -- The time at which the group is created.
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
    FOREIGN KEY (representative_group_id) REFERENCES scalar_groups (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE UNIQUE INDEX scalar_groups_representative_group_id ON scalar_groups (representative_group_id);
