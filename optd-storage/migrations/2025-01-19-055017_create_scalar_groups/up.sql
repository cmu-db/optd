CREATE TABLE scalar_groups (
  -- The group identifier
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  -- The optimization status of the group. 
  -- It could be: 
  -- Unexplored, Exploring, Explored, Optimizing, Optimized.
  -- `0` indicates `Unexplored`.
  status INTEGER NOT NULL,
  -- Time at which the group is created.
  created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
  -- The group identifier of the representative.
  rep_id BIGINT,
  FOREIGN KEY (rep_id) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);
