CREATE TABLE logical_props (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The relational group that shares this logical property entry.
    group_id BIGINT NOT NULL,
    -- The number of rows produced by this relation.
    card_est BIGINT NOT NULL,

    FOREIGN KEY(group_id) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);
