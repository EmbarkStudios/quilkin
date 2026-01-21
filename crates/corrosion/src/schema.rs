pub const SCHEMA: &str = r#"
CREATE TABLE servers (
    -- hostname or IP + port
    endpoint varchar(264) not null primary key,
    -- icao code
    icao char(4) not null default 'XXXX',
    -- Token set. Since SQLite does not support arrays, we use a base64 encoded binary blob
    tokens text,
    -- The JSONB set of peers that contributed this server
    contributors blob,
    -- The timestamp of the last contributors update, either insertion or deletion
    cont_update timestamp
);

CREATE TABLE dc (
    -- the IPv6 (or IPv4 mapped) address
    ip varchar(40) not null primary key,
    -- the QCMP port used for pinging
    port int not null default 0,
    -- icao code
    icao char(4) not null default 'XXXX',
    -- the JSONB set of servers that this peer contributed
    servers blob
);

CREATE TABLE filter (
    -- no sense making the filter itself the key
    id int not null primary key,
    -- the filter value. There is only ever one.
    filter text
);
"#;
