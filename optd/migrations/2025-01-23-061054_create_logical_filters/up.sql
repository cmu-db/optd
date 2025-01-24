-- Registers the logical filter operator.
CREATE TABLE logical_filters (
    logical_expr_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the child.
    child BIGINT NOT NULL,
    -- The filter predicate (e.g. <colA> > 3) (mocked).
    predicate TEXT NOT NULL,
    FOREIGN KEY (logical_expr_id) REFERENCES logical_exprs(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (child) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO logical_op_kinds (name) VALUES ('LogicalFilter');
