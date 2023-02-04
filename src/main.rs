
use std::process::Command;

use duckdb::{Connection, Result};
// use chrono::{DateTime, Utc};
use chrono::NaiveDate;


#[derive(Debug)]
struct BekbTransaction {
    credit_advice: String,
    transaction_date: NaiveDate, //DateTime,
    posting_date: NaiveDate, //DateTime, 
    description: String,
    additional_info: String,
    merchant_name: String,
    merchant_address: String,
    merchant_bank: String,
    reference_number: String,
    additional_info_transaction: String,
    amount_chf: f64,
    saldo_chf: f64,
}

impl std::fmt::Display for BekbTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Transaction {{ \
                    credit_advice: {}, \
                    transaction_date: {}, \
                    posting_date: {}, \
                    description: {}, \
                    additional_info: {}, \
                    merchant_name: {}, \
                    merchant_address: {}, \
                    merchant_bank: {}, \
                    reference_number: {}, \
                    additional_info_transaction: {}, \
                    amount_chf: {}, \
                    saldo_chf: {} \
                }}", 
                self.credit_advice, 
                self.transaction_date, 
                self.posting_date, 
                self.description, 
                self.additional_info, 
                self.merchant_name, 
                self.merchant_address, 
                self.merchant_bank, 
                self.reference_number, 
                self.additional_info_transaction, 
                self.amount_chf, 
                self.saldo_chf)
    }
}

fn connect_duckdb(path: &str) -> Result<Connection> {
    //If the database file does not exist, it will be created (the file extension may be .db, .duckdb, or anything else).
    let conn = Connection::open(&path);
    // let conn = Connection::open_in_memory();
    // println!("{}", conn.is_autocommit());
    println!("{}", path);
    conn
}


fn main() -> Result<()> {

    let path_data = "/Users/sspaeti/Documents/duckdbs/duckdb-rs/";
    let path_data_bekb = "/Users/sspaeti/Simon/Sync/1 Areas/Finance/Bank/Kontoauszüge/_export/bekb/test/";
    // let path_data_bekb = "/Users/sspaeti/Simon/Sync/1 Areas/Finance/Bank/Kontoauszüge/_export/bekb/";
    let post_fix_csv = "test-data/test.csv";
    let table_name_bekb = "bekb_transactions";

    let conn = connect_duckdb(&format!("{}{}", path_data, "test.duckdb"))?;


    conn.execute_batch(
        &format!("CREATE OR REPLACE TABLE {} (
             \"credit_advice\"                 VARCHAR
            ,\"transaction_date\"              DATE
            ,\"posting_date\"                  DATE   
            ,\"description\"                   VARCHAR
            ,\"additional_info\"               VARCHAR
            ,\"merchant_name\"                 VARCHAR
            ,\"merchant_address\"              VARCHAR
            ,\"merchant_bank\"                 VARCHAR
            ,\"reference_number\"              VARCHAR
            ,\"additional_info_transaction\"   VARCHAR
            ,\"amount_chf\"                    DOUBLE 
            ,\"saldo_chf\"                     DOUBLE 
         ); 
    ", table_name_bekb))?;
    
    // iterate over all xls files and convert to csv
    let paths = std::fs::read_dir(&path_data_bekb).unwrap();
    for path in paths {
        //print path
        let filepath = path.unwrap().path();
        let filename = filepath.display().to_string();
        let filetype = filepath.extension().unwrap().to_str().unwrap();

        if filetype == "xls" {

            println!("converting to csv: {:?} ..", filename);

            // use openoffice cmd line tool unoconv (`brew install unoconv`)
            let output = Command::new("sh")
                        .arg("-c")
                        .arg(&format!("unoconv -f csv \"{}\"", filename))
                        .output()
                        .expect("failed to execute process");

            let output_stdout = output.stdout;
            //print if not empty
            if !output_stdout.is_empty() {
                println!("stdout: {}", String::from_utf8_lossy(&output_stdout));
            }
        }
    }


    // iterate over all csv files and load into duckdb
    let paths = std::fs::read_dir(&path_data_bekb).unwrap();
    for path in paths {
        //print path
        let filepath = path.unwrap().path();
        let filename = filepath.display().to_string();
        let filetype = filepath.extension().unwrap().to_str().unwrap();

        if filetype == "csv" {
            println!("Inserting csv file to table `{}`: {:?} ..", table_name_bekb, filename);
            conn.execute_batch(&format!(r"COPY {} from '{}' (auto_detect true);", table_name_bekb, filename))?;
        }
    }
    
    // count rows
    let mut stmt = conn.prepare(&format!("
        SELECT count(*) as cnt
        FROM {};", table_name_bekb))?;

    //execute stmt.query_row() and print count
    let cnt: i64 = stmt.query_row([], |row| row.get(0))?;
    println!("{} rows in table {}", cnt, table_name_bekb);


    // // query table by rows
    // let mut stmt = conn.prepare(&format!("
    //     SELECT 
    //          COALESCE(credit_advice, NULL, '') as credit_advice
    //         ,transaction_date
    //         ,posting_date
    //         ,COALESCE(description, NULL, '') as description
    //         ,COALESCE(additional_info, NULL, '') as additional_info
    //         ,COALESCE(merchant_name, NULL, '') as merchant_name
    //         ,COALESCE(merchant_address, NULL, '') as merchant_address
    //         ,COALESCE(merchant_bank, NULL, '') as merchant_bank
    //         ,COALESCE(reference_number, NULL, '') as reference_number
    //         ,COALESCE(additional_info_transaction, NULL, '') as additional_info_transaction
    //         ,COALESCE(amount_chf, NULL, 0) as amount_chf
    //         ,COALESCE(saldo_chf, NULL, 0) as saldo_chf
    //     FROM {};", table_name_bekb))?;

    // let transaction_iter = stmt.query_map([], |row| {
    //     Ok(BekbTransaction {
    //         credit_advice: row.get("credit_advice")?,
    //         transaction_date: NaiveDate::from_ymd(row.get("transaction_date")?, 1, 1),
    //         posting_date: NaiveDate::from_ymd(row.get("posting_date")?, 1, 1),
    //         description: row.get("description")?,
    //         additional_info: row.get("additional_info")?,
    //         merchant_name: row.get("merchant_name")?,
    //         merchant_address: row.get("merchant_address")?,
    //         merchant_bank: row.get("merchant_bank")?,
    //         reference_number: row.get("reference_number")?,
    //         additional_info_transaction: row.get("additional_info_transaction")?,
    //         amount_chf: row.get("amount_chf")?,
    //         saldo_chf: row.get("saldo_chf")?,
    //     })
    // })?;

    // for transaction in transaction_iter {
    //         println!("{}", transaction.unwrap());
    //     }


    Ok(())




    // ARROW Example for bigger data
    // 
    // 
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
