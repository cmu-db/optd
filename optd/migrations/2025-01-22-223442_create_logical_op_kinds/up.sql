-- The logical operator descriptors table specifies all the 
-- logical operators that can be used in optimizer.
CREATE TABLE logical_op_kinds (
    -- The identifier of the logical operator.
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The name of the logical operator.
    name TEXT NOT NULL
);
