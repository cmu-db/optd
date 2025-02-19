CREATE TABLE events (
    -- A new epoch is created every time an event happens.
    epoch_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    -- The type of event that occurred (e.g. )
     -- TODO(yuchen): need more information than the event type to track what is modified.
    event_type TEXT NOT NULL,
    -- The time at which the event occurred.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
