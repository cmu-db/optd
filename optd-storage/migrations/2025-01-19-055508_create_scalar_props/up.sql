-- The scalar properties table contains the scalar property associated with 
-- some scalar expression.
-- TODO(yuchen): add scalar properties.
CREATE TABLE scalar_props (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    payload BLOB NOT NULL
);