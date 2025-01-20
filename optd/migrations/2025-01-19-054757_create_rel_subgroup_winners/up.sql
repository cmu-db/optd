-- The winners table records the winner of a group with some required physical property.
CREATE TABLE rel_subgroup_winners (
  -- The subgroup id of the winner, i.e. the winner of the group with `group_id` and some required physical property.
  subgroup_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  -- The physical expression id of the winner.
  physical_expr_id BIGINT NOT NULL,
  FOREIGN KEY (subgroup_id) REFERENCES rel_subgroup(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (physical_expr_id) REFERENCES physical_exprs(id) ON DELETE CASCADE ON UPDATE CASCADE
);
