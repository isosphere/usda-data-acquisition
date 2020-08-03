use crate::usda::USDADataPackage;
use crate::usda::datamart::DatamartConfig;
use postgres::types::ToSql;

use chrono::NaiveDate;

pub fn insert_usda_package(package: USDADataPackage, structure: &DatamartConfig, client: &mut postgres::Client) -> Result<usize, postgres::Error> {
    let report_name = package.name;

    for (section, results) in package.sections {
        // Dynamic statement preparation
        // warning: this SQL construction is sensitive magic and prone to breaking
        let table_name = match &structure.sections[&section].alias {
            Some(alias) => {format!("{}_{}", report_name, alias).to_owned()},
            None => {format!("{}_{}", report_name, section).to_owned()}
        }.to_lowercase();

        let independent = &structure.sections[&section].independent;
        let mut sql = format!(r#"INSERT INTO {table_name} (report_date, "#, table_name=&table_name).to_owned();
        
        for column in &independent[1..] {
            sql.push_str(&format!("\"{}\", ", column));
        }
        sql.push_str("variable_name, value, value_text) VALUES(");
        for i in 1..=independent.len()+3 {
            sql.push_str(&format!("${},", i));
        }
        sql.pop();
        sql.push_str(&format!(") ON CONFLICT ON CONSTRAINT {table_name}_pkeys DO NOTHING", table_name=table_name));

        //println!("{}", sql);
        
        let statement = client.prepare(&sql).unwrap();
        
        // Data processing and insertion
        for usda_package in results {
            let report_date = usda_package.report_date;
            let independent = &usda_package.independent;

            for (key, value) in usda_package.entries {
                let value_numeric = {
                    let temp = value.replace(",", "");
                    match temp.parse::<f32>() {
                        Ok(v) => { Some(v) },
                        Err(_) => { None }
                    }
                };
                if !value.is_empty() {
                    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new(); // this is some kind of magic that i do not yet understand
                    
                    params.push(&report_date);
                    for column in &independent[1..] {
                        params.push(column);
                    }
                    params.push(&key);
                    params.push(&value_numeric);
                    params.push(&value);

                    //println!("{:?}", params);

                    client.execute(&statement, &params[..]).unwrap();
                }
            }
        }
    }
    Ok(0)
}

pub fn find_maximum_existing_datamart_date(current_config: &DatamartConfig, client: &mut postgres::Client) -> Result<NaiveDate, String> {
    let mut max_date_found: Option<NaiveDate> = None;

    for section in current_config.sections.keys() {
        let table_name = match &current_config.sections[section].alias {
            Some(alias) => {format!("{}_{}", current_config.name, alias).to_owned()},
            None => {format!("{}_{}", current_config.name, section).to_owned()}
        }.to_lowercase();

        let sql = format!("SELECT MAX(report_date) FROM {}", table_name);
        let statement = match client.prepare(&sql) {
            Ok(s) => {s},
            Err(e) => {
                return Err(format!("Failed to prepare statement: `{}`, error: `{}`", sql, e))
            }
        };
    
        let row = client.query_one(&statement, &[]);

        match row {
            Ok(v) => { 
                let value: Option<NaiveDate> = v.get(0);

                match (max_date_found, value) {
                    (Some(date), Some(value)) => {
                        if date < value {
                            max_date_found = Some(value);
                        }
                    },
                    (None, Some(value)) => {max_date_found = Some(value);},
                    (Some(_), None) => {},
                    (None, None) => {}
                }
            },
            Err(_) => {
                return Err(format!("Failed to obtain latest data for {}, aborting.", table_name))
            }
        }
    }

    match max_date_found {
        Some(d) => { Ok(d) },
        None => { Err(String::from("No date found"))}
    }
}
