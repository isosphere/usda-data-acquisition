use crate::noaa;
use crate::usda;
use crate::usda::{USDADataPackage, USDADataPackageSection};

use std::collections::{HashMap, HashSet};
use chrono::NaiveDate;
use std::convert::TryInto;

lazy_static! {
    pub static ref SUPPORTED_NOAA_ELEMENTS: HashSet<&'static str> = [
        /*
        "PRCP", "SNOW", "SNWD", "TMAX", "TMIN", "ACMC", "ACMH", "ACSC", "ACSH", "AWDR",
        "AWND", "DAEV", "DAPR", "DASF", "DATN", "DATX", "DAWM", "DWPR", "EVAP", "FMTM",
        "FRGB", "FRGT", "FRTH", "GAHT", "MDEV", "MDPR", "MDSF", "MDTN", "MDTX", "MDWM",
        "MNPN", "MXPN", "PGTM", "PSUN", "SN01", "SN02", "SN03", "SN04", "SN05", "SN06",
        "SN07", "SN11", "SN21", "SN31", "SN41", "SN51", "SN61", "SN71", "SN81", "SN12",
        "SN22", "SN32", "SN42", "SN52", "SN62", "SN72", "SN82", "SN13", "SN23", "SN33",
        "SN43", "SN53", "SN63", "SN73", "SN83", "SN14", "SN24", "SN34", "SN44", "SN54",
        "SN64", "SN74", "SN84", "SN15", "SN25", "SN35", "SN45", "SN55", "SN65", "SN75",
        "SN85", "SN16", "SN26", "SN36", "SN46", "SN56", "SN66", "SN76", "SN86", "SN17",
        "SN27", "SN37", "SN47", "SN57", "SN67", "SN77", "SN87", "SX01", "SX02", "SX03",
        "SX04", "SX05", "SX06", "SX07", "SX11", "SX21", "SX31", "SX41", "SX51", "SX61",
        "SX71", "SX81", "SX12", "SX22", "SX32", "SX42", "SX52", "SX62", "SX72", "SX82",
        "SX13", "SX23", "SX33", "SX43", "SX53", "SX63", "SX73", "SX83", "SX14", "SX24",
        "SX34", "SX44", "SX54", "SX64", "SX74", "SX84", "SX15", "SX25", "SX35", "SX45",
        "SX55", "SX65", "SX75", "SX85", "SX16", "SX26", "SX36", "SX46", "SX56", "SX66",
        "SX76", "SX86", "SX17", "SX27", "SX37", "SX47", "SX57", "SX67", "SX77", "SX87",
        "TAVG", "THIC", "TOBS", "TSUN", "WDF1", "WDF2", "WDF5", "WDFG", "WDFI", "WDFM",
        "WDMV", "WESD", "WESF", "WSF1", "WSF2", "WSF5", "WSFG", "WSFI", "WSFM", "WT01",
        "WT02", "WT03", "WT04", "WT05", "WT06", "WT07", "WT08", "WT09", "WT10", "WT11",
        "WT12", "WT13", "WT14", "WT15", "WT16", "WT17", "WT18", "WT19", "WT21", "WT22",
        "WV01", "WV03", "WV07", "WV18", "WV20"*/
        "TMAX", "TMIN", "TAVG", "EVAP"
    ].iter().cloned().collect();
}

impl From<Vec<noaa::Observation>> for USDADataPackage {
    fn from(package: Vec<noaa::Observation>) -> Self {
        let mut output_package = USDADataPackage::new("NOAA".to_owned());

        for observation in package {
            if !SUPPORTED_NOAA_ELEMENTS.contains(&(observation.element.as_str())) {
                println!("Skipping unsupported element: {}", observation.element);
                continue;
            }
            for (day, data) in observation.observations.iter().enumerate() {
                // if the value is empty, don't bother with this record
                let value_string = match data.value.as_ref() {
                    Some(v) => { v.to_string() },
                    None => { continue }
                };

                let this_date = NaiveDate::from_ymd(
                    observation.year.try_into().unwrap(),
                    observation.month.try_into().unwrap(),
                    (day + 1).try_into().unwrap()
                );

                let mut destination_section = USDADataPackageSection::new(this_date);
                destination_section.independent.push(this_date.format("%Y-%m-%d").to_string());
                destination_section.independent.push(observation.station_id.to_owned());
                
                let measure_string = match data.measure_flag.as_ref() {
                    Some(v) => {v.to_string()},
                    None => {"".to_owned()}
                };
                
                destination_section.entries.insert(
                    "measure_flag".to_owned(),
                    measure_string
                );

                let quality_string = match data.quality_flag.as_ref() {
                    Some(v) => { v.to_string() },
                    None => {"".to_owned()}
                };

                destination_section.entries.insert(
                    "quality_flag".to_owned(),
                    quality_string
                );

                destination_section.entries.insert(
                    "source_flag".to_owned(),
                    data.source_flag.to_owned()
                );

                destination_section.entries.insert(
                    "value".to_owned(),
                    value_string
                );

                let element = output_package.sections.entry(observation.element.to_owned()).or_insert_with(Vec::new);
                element.push(destination_section);
            }
        }

        output_package
    }
}

#[test]
fn test_from_noaa() {
    use tar::{Builder, Header};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::prelude::*;
    use std::io::Cursor;

    // note: this data is made up so that we see a variety in the response, so don't worry about weird flags
    let test_string = r#"AE000041196194403TAVG-9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999     292H S  274H S  242H S  250H S  263H S  257H S  233H S  239H S  217H S  245H S  292H S  260H S
AE000041196194404TMAX  258  I  263  I  258  I  263  I  296  I  302  I  358  I  391  I  380  I  308  I  291  I  274  I  280  I  369  I  330 KI  335B I  385  I  385  I  374  I  374  I  313  I  308  I  308  I  302  I  313  I  330  I  335  I  302  I  313  I  346  I-9999   
AE000041196194404TMIN  180  I  180  I  163  I  146  I  135  I-9999   -9999     196  I  235  I  213  I  163  I-9999     180  I  174  I-9999     196  I  241  I  235  I  208  I  196  I  208  I  213  I  180  I  174  I  180  I  180  I  169  I  152  I  169  I  169  I-9999   
"#;

    let cursor = Cursor::new(test_string);
    
    let mut header = Header::new_gnu();
    header.set_path("foo.dly").unwrap();
    header.set_size(test_string.len().try_into().unwrap());
    header.set_cksum();
    
    let mut archive = Builder::new(Vec::new());
    archive.append(&header, cursor).unwrap();
    let archive = archive.into_inner().unwrap();

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&archive[..]).unwrap();

    let result = encoder.finish().unwrap();
    let cursor = Cursor::new(result);

    let results = noaa::process_noaa(cursor, None, None).unwrap();
    let converted_result = USDADataPackage::from(results);

    println!("{:#?}", converted_result)
}

/// A translation of the NOAA structure for the data-acquistion project
pub fn noaa_structure() -> usda::datamart::DatamartConfig {
    let mut sections: HashMap<String, usda::datamart::DatamartSection> = HashMap::new();
    for element in SUPPORTED_NOAA_ELEMENTS.iter() {
        let section = usda::datamart::DatamartSection {
            alias: None,
            independent: vec!["report_date".to_owned(), "station_id".to_owned()],
            fields: vec![
                "measure_flag".to_owned(), "source_flag".to_owned(), 
                "quality_flag".to_owned(), "value".to_owned()
            ]
        };
        sections.entry(String::from(*element)).or_insert(section);
    }

    usda::datamart::DatamartConfig {
        name: "NOAA".to_owned(),
        description: "National Oceanic and Atmospheric Administration Weather Data".to_owned(),
        independent: "report_date".to_owned(),
        sections
    }
}

#[test]
fn test_noaa_structure() {
    println!("{:?}", noaa_structure())
}

pub fn insert_noaa_package(observations: Vec<noaa::Observation>, client: &mut postgres::Client) -> Result<(), postgres::Error> {
    for observation in observations {
        if !SUPPORTED_NOAA_ELEMENTS.contains(&(observation.element.as_str())) {
            println!("Skipping unsupported element: {}", observation.element);
            continue;
        }

        let table_name = format!("noaa_{}", observation.element).to_owned();
        let sql = format!(r#"
            INSERT INTO {table_name} (report_date, station_id, variable_name, value, value_text) VALUES($1, $2, $3, $4, $5)
            ON CONFLICT ON CONSTRAINT {table_name}_pkeys DO NOTHING
        "#, table_name=&table_name).to_owned();

        //println!("{}", sql);
        
        let statement = client.prepare(&sql).unwrap();

        for (day, data) in observation.observations.iter().enumerate() {
            // if the value is empty, don't bother with this record
            let value_string = match data.value.as_ref() {
                Some(v) => { v.to_string() },
                None => { continue }
            };

            let this_date = NaiveDate::from_ymd(
                observation.year.try_into().unwrap(),
                observation.month.try_into().unwrap(),
                (day + 1).try_into().unwrap()
            );
            
            let measure_string = match data.measure_flag.as_ref() {
                Some(v) => {v.to_string()},
                None => {"".to_owned()}
            };
            
            let quality_string = match data.quality_flag.as_ref() {
                Some(v) => { v.to_string() },
                None => {"".to_owned()}
            };

            let empty_value: Option<f32> = None;

            client.execute(&statement, &[
                &this_date, &observation.station_id, &"quality_flag".to_owned(), &empty_value, &quality_string
            ])?;
            client.execute(&statement, &[
                &this_date, &observation.station_id, &"source_flag".to_owned(), &empty_value, &data.source_flag
            ])?;
            client.execute(&statement, &[
                &this_date, &observation.station_id, &"measure_flag".to_owned(), &empty_value, &measure_string
            ])?;

            let value_numeric: Option<f32> = match data.value.as_ref() {
                Some(v) => {Some(*v as f32)},
                None => {None}
            };

            client.execute(&statement, &[
                &this_date, &observation.station_id, &"value".to_owned(), &value_numeric, &value_string
            ])?;
        }
    }
    Ok(())
}