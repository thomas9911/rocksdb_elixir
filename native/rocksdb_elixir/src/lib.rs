use rocksdb::{Options, DB};
use rustler::{Binary, Env, NewBinary, NifStruct, ResourceArc};

type Error = String;
type Result = std::result::Result<RocksDBConnection, Error>;

#[derive(NifStruct)]
#[module = "RocksDBElixir.Conn"]
struct RocksDBConnection {
    path: String,
    resource: Option<ResourceArc<RocksDbResource>>,
}

impl RocksDBConnection {
    pub fn new(db: DB, path: String) -> Result {
        Ok(RocksDBConnection {
            path,
            resource: Some(ResourceArc::new(RocksDbResource { db })),
        })
    }

    pub fn resource(&self) -> std::result::Result<&ResourceArc<RocksDbResource>, Error> {
        match &self.resource {
            None => Err(String::from("resource closed")),
            Some(resource) => Ok(resource),
        }
    }
}

#[derive(Debug)]
struct RocksDbResource {
    db: DB,
}

impl std::ops::Deref for RocksDbResource {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn new(path: String) -> Result {
    let db = DB::open_default(&path).unwrap();

    RocksDBConnection::new(db, path)
}

#[rustler::nif(schedule = "DirtyIo")]
fn flush(conn: RocksDBConnection) -> std::result::Result<(), Error> {
    conn.resource()?.flush().map_err(|e| e.to_string())?;

    Ok(())
}

#[rustler::nif(schedule = "DirtyIo")]
fn close(conn: RocksDBConnection) -> std::result::Result<RocksDBConnection, Error> {
    let mut conn = conn;
    conn.resource = None;

    Ok(conn)
}

#[rustler::nif(schedule = "DirtyIo")]
fn destroy(path: &str) -> std::result::Result<(), Error> {
    DB::destroy(&Options::default(), path).map_err(|e| e.to_string())?;

    Ok(())
}

#[rustler::nif(schedule = "DirtyIo")]
fn put(conn: RocksDBConnection, key: Binary, value: Binary) -> Result {
    conn.resource()?
        .put(key.as_slice(), value.as_slice())
        .map_err(|e| e.to_string())?;

    Ok(conn)
}

#[rustler::nif(schedule = "DirtyIo")]
fn get<'a>(
    env: Env<'a>,
    conn: RocksDBConnection,
    key: Binary,
) -> std::result::Result<Option<Binary<'a>>, Error> {
    let output = match conn
        .resource()?
        .get_pinned(key.as_slice())
        .map_err(|e| e.to_string())?
    {
        Some(data) => {
            let mut binary = NewBinary::new(env, data.len());
            binary.as_mut_slice().copy_from_slice(&data);
            Some(binary.into())
        }
        None => None,
    };

    Ok(output)
}

#[rustler::nif(schedule = "DirtyIo")]
fn delete(conn: RocksDBConnection, key: Binary) -> Result {
    conn.resource()?
        .delete(key.as_slice())
        .map_err(|e| e.to_string())?;

    Ok(conn)
}

fn load(env: Env, _: rustler::Term) -> bool {
    rustler::resource!(RocksDbResource, env);
    true
}

rustler::init!(
    "Elixir.RocksDBElixir.Native",
    [new, flush, destroy, close, put, get, delete],
    load = load
);
