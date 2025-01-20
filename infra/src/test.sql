CREATE TABLE employees (
    id INTEGER,
    name TEXT,
    department_id INTEGER
);

CREATE TABLE departments (
    id INTEGER,
    department_name TEXT
);

INSERT INTO employees VALUES 
    (1, 'Alice', 1),
    (2, 'Bob', 2),
    (3, 'Charlie', 1);

INSERT INTO departments VALUES
    (1, 'Engineering'),
    (2, 'Marketing');

SELECT e.name, d.department_name
FROM employees e
JOIN departments d ON e.department_id = d.id;
