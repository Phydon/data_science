use std::path::Path;
use std::error::Error;

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
) -> Result<(), Box<dyn Error>> {
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
