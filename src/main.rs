use data_science::*;

const PATH_TO_EXCEL: &str = "./input/test.xlsx";
const PATH_TO_CSV: &str = "./output/test.csv";
const SHEETNAME: &str = "Sheet1";

fn main() {
    excel2csv(PATH_TO_EXCEL, SHEETNAME, PATH_TO_CSV);

    get_dataframe_from_csv(PATH_TO_CSV).unwrap();
}
