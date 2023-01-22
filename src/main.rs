use duckdb::{params, Connection, Result};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;

#[derive(Debug)]
struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>,
}

fn connect_duckdb(path: &str) -> Result<Connection> {
    //If the database file does not exist, it will be created (the file extension may be .db, .duckdb, or anything else).
    let conn = Connection::open(&path);
    // println!("{}", conn.is_autocommit());
    Ok(conn.unwrap())
}


fn main() -> Result<()> {
    // let conn = Connection::ope

    let conn = connect_duckdb("/tmp/duckdb-rs/test.duckdb")?;


    conn.execute_batch(
        r"CREATE SEQUENCE IF NOT EXISTS seq;
          CREATE TABLE person (
                  id              INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
                  name            TEXT NOT NULL,
                  data            BLOB
                  );
        ")?;

    let me = Person {
        id: 0,
        name: "Steven".to_string(),
        data: None,
    };
    conn.execute(
        "INSERT INTO person (name, data) VALUES (?, ?)",
        params![me.name, me.data],
    )?;

    // query table by rows
    let mut stmt = conn.prepare("SELECT id, name, data FROM person")?;
    let person_iter = stmt.query_map([], |row| {
        Ok(Person {
            id: row.get(0)?,
            name: row.get(1)?,
            data: row.get(2)?,
        })
    })?;

    for person in person_iter {
        println!("Found person {:?}", person.unwrap());
    }

    Ok(())
    // // query table by arrow
    // let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    // print_batches(&rbs).unwrap();
    // Ok(())

    // // query table by arrow
    // let frames = stmt.query_arrow(duckdb::params![])?;

    // let schema = frames.get_schema();
    // let mut records = Vec::new();
    // let mut _num_records = 0;

    // for frame in frames {
    //     _num_records += frame.num_rows();
    //     records.push(frame);
    // }
    // let row_count = stmt.row_count();
    // println!("row count {:?}", row_count);
}
