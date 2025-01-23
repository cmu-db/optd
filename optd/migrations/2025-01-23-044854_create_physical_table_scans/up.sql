-- Registers the physical table scan operator.
CREATE TABLE physical_table_scans (
    physical_expr_id INTEGER NOT NULL PRIMARY KEY,
    -- Ideally this will be an unique id of the table in the catalog,
    -- For now using table name to fake it.
    table_name TEXT NOT NULL,
    FOREIGN KEY (physical_expr_id) REFERENCES physical_expr_id(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO physical_op_descs (name) VALUES ('PhysicalTableScan');
