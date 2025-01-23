-- Registers the physical nested loop join operator.
CREATE TABLE physical_nljoins (
    physical_expr_id INTEGER NOT NULL PRIMARY KEY,
    -- The type of join (inner, left, right, etc.).
    join_type INTEGER NOT NULL,
    -- The group id of the left child.
    left BIGINT NOT NULL,
    -- The group id of the right child.
    right BIGINT NOT NULL,
    -- The join condition (mocked).
    join_cond TEXT NOT NULL,
    FOREIGN KEY (physical_expr_id) REFERENCES physical_expr_id(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (left) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (right) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO physical_op_descs (name) VALUES ('PhysicalNLJoin');
