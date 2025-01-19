-- The winners table records the winner of a group with some required physical property.
CREATE TABLE rel_group_winners (
  -- The group we are interested in.
  group_id BIGINT NOT NULL,
  -- Identified for the required physical property.
  required_phys_prop_id BIGINT NOT NULL,
  -- The winner of the group with `group_id` and required physical property.
  physical_expr_id BIGINT NOT NULL,
  PRIMARY KEY (group_id, required_phys_prop_id),
  FOREIGN KEY (group_id) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (required_phys_prop_id) REFERENCES physical_props(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (physical_expr_id) REFERENCES physical_exprs(id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Could also do a query to compute the winner: 
-- SELECT MIN(cost), [all other fields] 
-- FROM physical_exprs 
-- WHERE group_id = <input_group> and satisfies(derived_phys_prop_id, required_phys_prop_id);
