use data_science::*;

use std::collections::BTreeMap;

const PATH_TO_EXCEL: &str = "./input/input.xlsx";
const PATH_TO_CSV: &str = "./output/from_excel.csv";
const PATH_TO_CSV_DATA: &str = "./output/data.csv";
const SHEETNAME: &str = "Sheet1";

fn main() {
    excel2csv(PATH_TO_EXCEL, SHEETNAME, PATH_TO_CSV);

    let container: BTreeMap<i64, i64> =
        transform_data_from_csv(PATH_TO_CSV).unwrap();
    // println!("container: {:?}", container);

    write_data_to_csv(container, PATH_TO_CSV_DATA).expect(&format!(
        "Unable to write data to csv file {PATH_TO_CSV_DATA}"
    ));

    println!("Data written to csv file \"{PATH_TO_CSV_DATA}\"");
}
