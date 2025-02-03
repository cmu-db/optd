CREATE TABLE relation_groups (
    -- A unique identifier for a group of relational expressions in the memo table.
    id INTEGER NOT NULL PRIMARY KEY,
    -- The representative group id of a relation group. 
    -- At insertion time, the representative group id is the same as the id,
    -- but it can be updated later.
    representative_group_id BIGINT NOT NULL,
    -- The exploration status of a relation group. 
    -- It can be one of the following values: Unexplored, Exploring, Explored.
    exploration_status INTEGER NOT NULL,
    -- The time at which the group is created.
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,

    FOREIGN KEY (representative_group_id) REFERENCES relation_groups (id) 
    ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE UNIQUE INDEX relation_groups_representative_group_id ON relation_groups (representative_group_id);
