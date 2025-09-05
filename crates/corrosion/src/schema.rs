pub const SCHEMA: &str = r#"
CREATE TABLE servers (
    -- hostname or IP + port
    endpoint binary(264) not null primary key,
    -- icao code
    icao char(4) not null default 'XXXX',
    -- locality
    locality varchar(128),
    -- Token set. Since SQLite does not support arrays, we use a blob with a
    -- specific encoding
    tokens text,
);
"#;
