-- The scalar expressions table specifies which group a scalar expression belongs to.
-- It also specifies the derived scalar property and the cost associated with the 
CREATE TABLE scalar_exprs (
  -- The scalar expression id.
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  -- The type descriptor of the scalar expression.
  typ_desc BIGINT NOT NULL,
  -- The group identifier of the scalar expression.
  group_id BIGINT NOT NULL,
  -- Time at which the logical expression is created.
  created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
  -- The cost associated computing the scalar expression.
  cost REAL, -- TODO: This can be NULL, do we want a seperate table?
  FOREIGN KEY (typ_desc) REFERENCES scalar_typ_descs(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (group_id) REFERENCES scalar_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);
