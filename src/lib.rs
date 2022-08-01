use std::{
    path::Path,
    io,
    collections::BTreeMap
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
    convert_to_csv(container, path_to_csv).expect("Error while writing csv file {path_to_csv}");
}

fn set_value_to_zero<'a>(df: &'a mut DataFrame, column_with_pattern_name: &'a str, pattern_lst: Vec<&'a str>, column_to_apply: &'a str) -> Result<&'a DataFrame> {
    // set value in "column_to_apply" to zero if 
    // value in "column_with_pattern_name" matches "pattern"
    for pattern in pattern_lst {
        let col = df.column(column_with_pattern_name)?;
        let mask = col.equal(pattern)?;
        df.try_apply(column_to_apply, |val| {
            val.i64()?
            .set(&mask, Some(0))
        })?;
    }
        
    Ok(df)
}

pub fn transform_data_from_csv(path_to_csv: &str) -> Result<BTreeMap<i64,i64>> {
    // EAGER DF
    let mut df = CsvReader::from_path(path_to_csv)?
                .infer_schema(None)
                .has_header(true)
                .finish()?;
    // println!("Dataframe COLUMNS: {:?}", df.get_column_names());

    df.select(["column A1", "column A3", "column A4"])?;


    let pattern1 = vec!["wasd", "qwertz"];
    set_value_to_zero(&mut df, "column A1", pattern1, "column A4").expect("Error while trying to set pattern to zero");
    // println!("wasd & qwertz => {:?}", df.column("column A4").unwrap());

    let pattern2 = vec!["W154_1000"];
    set_value_to_zero(&mut df, "column A2", pattern2, "column A4").expect("Error while trying to set pattern to zero");
    // println!("wasd & qwertz => {:?}", df.column("column A4").unwrap());


    // group the same values in col A3 together
    // sum values in col A4 together grouped-by value from A3:
    let df_sum: DataFrame = df.groupby(["column A3"])?
        .select(["column A4"])
        .sum()?
        .sort(["column A3"], false)?;
    // println!("Grouped-by: {:?}", df_sum);

    // collect the grouped values and their sum in a BTreeMap 
    // for later use
    let mut vt_sum: BTreeMap<_,_> = BTreeMap::new();
    let left_col = df_sum.column("column A3")?.i64()?;
    let right_col = df_sum.column("column A4_sum")?.i64()?;

    let mut sum_storage: Vec<_> = left_col.into_iter()
        .zip(right_col.into_iter())
        .map(|(left_it, right_it)| match (left_it, right_it) {
            (Some(l), Some(r)) => vt_sum.insert(l, r),
            _ => None,
        })
        .collect();

    sum_storage.clear();
    // println!("btreemap: {:?}", vt_sum);

    Ok(vt_sum)
}

pub fn write_data_to_csv(container: BTreeMap<i64,i64>, path_to_csv_data: &str) -> io::Result<()> {
    let mut wtr = csv::Writer::from_path(path_to_csv_data)?;

    for (k,v) in container {
        let tmp = vec![k.to_string(), v.to_string()];
        wtr.write_record(&tmp)?;
    }

    wtr.flush()?;
    Ok(())

}
