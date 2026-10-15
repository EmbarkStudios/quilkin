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

CREATE TABLE node (
    -- the IPv6 (or IPv4 mapped) address, as observed by whoever it connected to
    ip varchar(40) not null primary key,
    -- the QCMP port it advertised
    port int not null default 0,
    -- icao code
    icao char(4) not null default 'XXXX',
    -- when it last joined
    joined timestamp,
    -- elements in each coordinate vector; 0 until the node has solved one. A
    -- reader MUST skip a row whose dim differs from its own
    dim int not null default 0,
    -- base64 of dim little endian f64s, as servers.tokens does for binary
    outgoing text,
    incoming text,
    -- the solve round the vectors came from, increasing per node
    round int not null default 0,
    -- when the vectors were last solved, for staleness
    solved timestamp
);

CREATE TABLE filter (
    -- no sense making the filter itself the key
    id int not null primary key,
    -- the filter value. There is only ever one.
    filter text
);
"#;
