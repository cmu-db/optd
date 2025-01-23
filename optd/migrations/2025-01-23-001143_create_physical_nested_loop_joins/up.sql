-- Registers the physical nested loop join operator.
CREATE TABLE physical_nested_loop_joins (
    physical_expr_id INTEGER NOT NULL PRIMARY KEY,
    -- The group id of the left child.
    left INTEGER NOT NULL,
    -- The group id of the right child.
    right INTEGER NOT NULL,
    FOREIGN KEY (physical_expr_id) REFERENCES physical_expr_id(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (left) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (right) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO physical_op_descs (name) VALUES ('PhysicalNestedLoopJoin');
