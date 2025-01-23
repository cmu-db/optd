-- Registers the logical scan operator.
CREATE TABLE logical_scans (
    logical_expr_id INTEGER NOT NULL PRIMARY KEY,
    -- Ideally this will be an unique id of the table in the catalog,
    -- For now using table name to fake it.
    table_name TEXT NOT NULL,
    FOREIGN KEY (logical_expr_id) REFERENCES logical_exprs(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO logical_op_descs (name) VALUES ('LogicalScan');
