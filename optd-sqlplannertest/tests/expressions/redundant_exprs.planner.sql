-- PAVLO
-- Setup Test Table
CREATE TABLE xxx (a INTEGER, b INTEGER);
INSERT INTO xxx VALUES (0, 0), (1, 1), (2, 2);
SELECT * FROM xxx WHERE a = 0;

/*
3
0 0
*/

-- (no id or description)
SELECT * FROM xxx WHERE a + 0 = b + 0;

/*
0 0
1 1
2 2
*/

