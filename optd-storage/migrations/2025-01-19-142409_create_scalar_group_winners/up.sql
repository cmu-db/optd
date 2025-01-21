-- The scalar group winners table records the winner of a scalar group.
CREATE TABLE scalar_group_winners (
  -- The scalar group we are interested in.
  group_id BIGINT NOT NULL,
  -- The winner of the group with `group_id`.
  scalar_expr_id BIGINT NOT NULL,
  PRIMARY KEY (group_id),
  FOREIGN KEY (group_id) REFERENCES scalar_groups(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (scalar_expr_id) REFERENCES scalar_exprs(id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Could also do a query to compute the winner: 
-- SELECT MIN(cost), [all other fields] 
-- FROM scalar_exprs 
-- WHERE group_id = <input_group>;
