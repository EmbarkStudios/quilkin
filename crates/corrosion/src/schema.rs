pub const SCHEMA: &str = r#"
CREATE TABLE servers (
    -- hostname or IP + port
    endpoint varchar(264) not null primary key,
    -- icao code
    icao char(4) not null default 'XXXX',
    -- locality
    locality varchar(128),
    -- Token set. Since SQLite does not support arrays, we use a base64 encoded
    -- binary blob
    tokens text,
);

CREATE TABLE dc (
    -- the IPv4 or IPv6 address
    ip varchar(40) not null primary key,
    -- the QCMP port used for pinging
    port int not null,
    -- icao code
    icao char(4) not null default 'XXXX',
);

CREATE TABLE filter (
    -- no sense making the filter itself the key
    id int not null primary key,
    -- the filter value. There is only ever one.
    filter text not null,
);
"#;
