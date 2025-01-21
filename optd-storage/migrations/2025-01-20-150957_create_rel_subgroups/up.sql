-- The relational subgroups table specifies the subgroups of a group with some required physical property.
CREATE TABLE rel_subgroups (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  -- The group the subgroup belongs to.
  group_id BIGINT NOT NULL,
  -- The required physical property of the subgroup.
  required_phys_prop_id BIGINT NOT NULL,
  FOREIGN KEY (group_id) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (required_phys_prop_id) REFERENCES physical_props(id) ON DELETE CASCADE ON UPDATE CASCADE
);