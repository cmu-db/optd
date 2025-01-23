-- Registers the logical join operator.
CREATE TABLE logical_joins (
    logical_expr_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the left child.
    left INTEGER NOT NULL,
    -- The group id of the right child.
    right INTEGER NOT NULL,
    FOREIGN KEY (logical_expr_id) REFERENCES logical_exprs(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (left) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (right) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO logical_op_descs (name) VALUES ('LogicalJoin');
