CREATE TABLE employees (
    id BIGINT,
    name TEXT,
    department_id BIGINT
);

CREATE TABLE departments (
    id BIGINT,
    department_name TEXT
);

INSERT INTO employees VALUES 
    (1, 'Alice', 1),
    (2, 'Bob', 2),
    (3, 'Charlie', 1);

INSERT INTO departments VALUES
    (1, 'Engineering'),
    (2, 'Marketing');


explain SELECT * FROM employees WHERE id = 2;

SELECT * FROM employees WHERE id = 2;
