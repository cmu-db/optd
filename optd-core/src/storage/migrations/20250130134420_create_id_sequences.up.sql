-- Table for id sequence generation. This is used to generate unique ids for various entities in the system.
-- In other words, this table only contains one row.
CREATE TABLE id_sequences (
    -- The id of the sequence.
    id INTEGER NOT NULL PRIMARY KEY,
    -- The current value of the id sequence.
    current_value BIGINT NOT NULL 
);

-- Currently, the entire system uses a single sequence for all ids.
INSERT INTO id_sequences (id, current_value) VALUES (0, 0);
