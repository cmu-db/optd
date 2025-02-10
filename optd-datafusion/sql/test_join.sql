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


SELECT * FROM employees JOIN departments WHERE employees.department_id = departments.id;