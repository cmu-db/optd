-- The relational subgroup expressions table specifies the physical expressions of a subgroup.
-- It is a m:n junction table since a subgroup can have multiple physical expressions, 
-- and a physical expression can belong to multiple subgroups.
CREATE TABLE rel_subgroup_physical_exprs (
  -- The subgroup the physical expression belongs to.
  subgroup_id BIGINT NOT NULL,
  -- The physical expression id.
  physical_expr_id BIGINT NOT NULL,
  PRIMARY KEY (subgroup_id, physical_expr_id),
  FOREIGN KEY (subgroup_id) REFERENCES rel_subgroups(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (physical_expr_id) REFERENCES physical_exprs(id) ON DELETE CASCADE ON UPDATE CASCADE
);
