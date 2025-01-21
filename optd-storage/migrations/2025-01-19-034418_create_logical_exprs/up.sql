-- The relational logical expressions table specifies which group a logical expression belongs to.
CREATE TABLE logical_exprs (
  -- The logical expression id.
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  -- The type descriptor of the logical expression.
  typ_desc BIGINT NOT NULL,
  -- The group identifier of the logical expression.
  group_id BIGINT NOT NULL, -- groups.id
  -- Time at which the logical expression is created.
  created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
  FOREIGN KEY (typ_desc) REFERENCES logical_typ_descs(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (group_id) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);
