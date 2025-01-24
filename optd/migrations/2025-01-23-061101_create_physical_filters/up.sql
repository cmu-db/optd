-- Registers the physical filter operator.
CREATE TABLE physical_filters (
    physical_expr_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the child.
    child BIGINT NOT NULL,
    -- The predicate to filter on (e.g. <colA> > 3) (mocked).
    predicate TEXT NOT NULL,
    FOREIGN KEY (physical_expr_id) REFERENCES physical_exprs(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (child) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO physical_op_kinds (name) VALUES ('PhysicalFilter');
