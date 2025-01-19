-- The relational physical expressions table specifies which group a physical expression belongs to.
-- It also specifies the derived physical property and the cost associated with the physical expression.
CREATE TABLE physical_exprs (
  -- The physical expression id.
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  -- The type descriptor of the physical expression.
  typ_desc BIGINT NOT NULL,
  -- The group this physical expression belongs to.
  group_id BIGINT NOT NULL,  -- groups.id
  -- The physical property dervied based on the properties of the children nodes.
  derived_phys_prop_id BIGINT NOT NULL,
  -- The cost associated with the physical expression.
  cost REAL NOT NULL,
  -- Time at which the physical expression is created.
  created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
  FOREIGN KEY (typ_desc) REFERENCES physical_typ_descs(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (group_id) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (derived_phys_prop_id) REFERENCES physical_props(id) ON DELETE CASCADE ON UPDATE CASCADE
);
