use std::collections::{HashMap, HashSet};
use std::convert::TryInto;

use chrono::NaiveDate;
use crate::noaa;
use crate::datamart;

#[derive(Debug)]
pub struct USDADataPackageSection {
    pub report_date: NaiveDate,
    pub independent: Vec<String>,
    pub entries: HashMap<String, String>
}

impl USDADataPackageSection {
    pub fn new(report_date: NaiveDate) -> USDADataPackageSection {
        USDADataPackageSection {
            report_date,
            independent: Vec::new(),
            entries: HashMap::new()
        }
    }
}

#[derive(Debug)]
pub struct USDADataPackage {
    pub name: String,
    pub sections: HashMap<
        String, // section name
        Vec<USDADataPackageSection>
    >,
}

impl USDADataPackage {
    pub fn new(name: String) -> USDADataPackage {
        USDADataPackage {
            name,
            sections: HashMap::new(),
        }
    }
}

lazy_static! {
    static ref SUPPORTED_NOAA_ELEMENTS: HashSet<&'static str> = [
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
        "WV01", "WV03", "WV07", "WV18", "WV20"
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
                    "measure_flag".to_owned(),
                    quality_string
                );

                destination_section.entries.insert(
                    "source_flag".to_owned(),
                    data.source_flag.to_owned()
                );

                let value_string = match data.value.as_ref() {
                    Some(v) => { v.to_string() },
                    None => { "".to_owned() }
                };

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

/// A translation of the NOAA structure for the data-acquistion project
pub fn noaa_structure() -> datamart::DatamartConfig {
    let mut sections: HashMap<String, datamart::DatamartSection> = HashMap::new();
    for element in SUPPORTED_NOAA_ELEMENTS.iter() {
        let section = datamart::DatamartSection {
            independent: vec!["report_date".to_owned(), "station_id".to_owned()],
            fields: vec![
                "measure_flag".to_owned(), "source_flag".to_owned(), 
                "quality_flag".to_owned(), "value".to_owned()
            ]
        };
        sections.entry(String::from(*element)).or_insert(section);
    }

    datamart::DatamartConfig {
        name: "NOAA".to_owned(),
        description: "National Oceanic and Atmospheric Administration Weather Data".to_owned(),
        independent: "report_date".to_owned(),
        sections
    }
}
