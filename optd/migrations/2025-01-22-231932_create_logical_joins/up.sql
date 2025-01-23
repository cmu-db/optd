-- Registers the logical join operator.
CREATE TABLE logical_joins (
    logical_expr_id INTEGER NOT NULL PRIMARY KEY,
    -- The type of join (inner, left, right, etc.).
    join_type INTEGER NOT NULL,
    -- The group id of the left child.
    left BIGINT NOT NULL,
    -- The group id of the right child.
    right BIGINT NOT NULL,
    -- The join condition (mocked).
    join_cond TEXT NOT NULL,
    FOREIGN KEY (logical_expr_id) REFERENCES logical_exprs(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (left) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (right) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO logical_op_descs (name) VALUES ('LogicalJoin');
