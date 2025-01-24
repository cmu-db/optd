-- The relational physical expressions table specifies which group 
-- a physical expression belongs to and the total cost for executing 
-- a physical plan rooted at this expression.
CREATE TABLE physical_exprs (
    -- The physical expression id.
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The physical operator descriptor id.
    physical_op_kind_id BIGINT NOT NULL,
    -- The group this physical expression belongs to.
    group_id BIGINT NOT NULL,
    -- The total cost for executing a physical plan rooted at this expression (FAKE).
    total_cost DOUBLE NOT NULL,
    -- Time at which the physical expression is created.
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
    FOREIGN KEY (physical_op_kind_id) REFERENCES physical_op_kinds(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (group_id) REFERENCES rel_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);
