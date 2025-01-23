-- The physical operator descriptor table stores all the 
-- physical operators that can be used in the optimizer.
CREATE TABLE physical_op_descs (
    -- The identifier of the physical operator.
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The name of the physical operator.
    name TEXT NOT NULL
);
