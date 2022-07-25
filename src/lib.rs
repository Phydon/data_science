use std::{
    path::Path,
    io
};

use polars::prelude::*;

fn read_it_all(path_to_excel: &str, sheetname: &str) -> Vec<Vec<String>> {
    let path = Path::new(path_to_excel);
    let book = umya_spreadsheet::reader::xlsx::read(path)
        .expect(&format!("Unable to open file: {}", path_to_excel));

    let max_col_and_row: (u32, u32) = book
        .get_sheet_by_name(sheetname)
        .expect(&format!("Unable to open sheet: {}", sheetname))
        .get_highest_column_and_row();

    let mut container: Vec<Vec<String>> = Vec::new();
    for i in 1..=max_col_and_row.1 {
        let mut line_storage: Vec<String> = Vec::new();
        for j in 1..=max_col_and_row.0 {
            let val = book
                .get_sheet_by_name(sheetname)
                .expect(&format!("Unable to open sheet: {}", sheetname))
                .get_value_by_column_and_row(&j, &i);
            line_storage.push(val);
        }
        container.push(line_storage);
    }

    container
}

fn convert_to_csv(
    container: Vec<Vec<String>>,
    path_to_csv: &str,
) -> io::Result<()> {
    let mut wtr = csv::Writer::from_path(path_to_csv)?;

    for line in container {
        wtr.write_record(&line)?;
    }

    wtr.flush()?;
    Ok(())
}

pub fn excel2csv(path_to_excel: &str, sheetname: &str, path_to_csv: &str) {
    let container = read_it_all(path_to_excel, sheetname);
    match convert_to_csv(container, path_to_csv) {
        Ok(()) => println!("CSV written"),
        Err(r) => eprintln!("Error while writing CSV: {r}"),
    }
}

pub fn get_dataframe_from_csv(path_to_csv: &str) -> Result<()> {
    // EAGER DF
    let mut df = CsvReader::from_path(path_to_csv)?
                .infer_schema(None)
                .has_header(true)
                .finish()?;

    // println!("Dataframe COLUMNS: {:?}", df.get_column_names());

    df.select(["column A1", "column A3", "column A4"])?;


    // set value in column A4 to zero if 
    // value in column A1 == "wasd" or "qwertz"
    let col_a1 = df.column("column A1")?;
    let mask_a1 = col_a1.equal("wasd")? | col_a1.equal("qwertz")?;
    df.try_apply("column A4", |val| {
        val.i64()?
        .set(&mask_a1, Some(0))
    })?;
        
    // println!("wasd & qwertz => {:?}", df.column("column A4").unwrap());


    // group the same values in col A3 together
    // sum values in col A4 together grouped-by value from A3:
    let df_sum: DataFrame = df.groupby(["column A3"])?
        .select(["column A4"])
        .sum()?
        .sort(["column A3"], false)?;

    println!("Grouped-by: {:?}", df_sum);


    Ok(())
}
