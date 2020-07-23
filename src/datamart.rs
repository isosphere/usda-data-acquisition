use std::collections::HashMap;
use std::sync::Arc;

use chrono::{NaiveDate, Local};
use regex::Regex;
use serde_derive::Deserialize; 

use crate::common::{USDADataPackage, USDADataPackageSection};

#[derive(Deserialize, Debug)]
pub struct DatamartSection {
    pub independent: Vec<String>, // first is always interpreted as a NaiveDate, following are text.
    pub fields: Vec<String>       // all will be attempted as numeric
}

#[derive(Deserialize, Debug)]
pub struct DatamartConfig {
    pub name: String,                             // historical "slug name"
    pub description: String,
    pub independent: String,                      // the independent variable, i.e.: date for query
    pub sections: HashMap<String, DatamartSection> 
}

#[derive(Deserialize, Debug)]
pub struct DatamartResponse {
    #[serde(rename(deserialize = "reportSection"))]
    report_section: String,
    #[serde(rename(deserialize = "reportSections"))]
    report_sections: Vec<String>,
    stats: HashMap<String, u32>,
    results: Vec<HashMap<String, Option<String>>>
}    

pub fn process_datamart(slug_id: String, report_date:Option<NaiveDate>, config: &HashMap<String, DatamartConfig>, http_connect_timeout:Arc<u64>, http_receive_timeout:Arc<u64>, minimum_date:Option<NaiveDate>) -> Result<USDADataPackage, String> {
    if !config.contains_key(&slug_id) {
        return Err(String::from(format!("Slug ID {} is not known to our datamart configuration.", slug_id)));
    }

    let report_label = &config.get(&slug_id).unwrap().name;
    let mut result = USDADataPackage::new(String::from(report_label));

    for section in config[&slug_id].sections.keys() {
        let section_data = result.sections.entry(String::from(section)).or_insert(Vec::new());

        let target_url = {
            let base_url = format!("https://mpr.datamart.ams.usda.gov/services/v1.1/reports/{}", slug_id);
            match report_date {
                Some(d) => {
                    format!(
                        "{base_url}/{section}?q={independent}={report_date}", 
                        base_url=base_url,
                        section=section,
                        independent=config[&slug_id].independent,
                        report_date=d.format("%m/%d/%Y")
                    )
                },
                None => {
                    match minimum_date {
                        Some(md) => {
                            let today = Local::now().naive_local().date();

                            format!(
                                "{base_url}/{section}?q={independent}={minimum_date}:{today}", 
                                base_url=base_url,
                                section=section,
                                independent=config[&slug_id].independent,
                                today=today.format("%m/%d/%Y"),
                                minimum_date=md.format("%m/%d/%Y")
                            )
                        },
                        None => {format!("{base_url}/{section}", base_url=base_url, section=section)}
                    }
                }
            }
        };

        let response = ureq::get(&target_url).timeout_connect(*http_connect_timeout).timeout_read(*http_receive_timeout).call();
        
        if let Some(error) = response.synthetic_error() {
            return Err(String::from(format!("Failed to retrieve data from datamart server with URL {}. Error: {}", target_url, error)));
        }

        let parsed = {
            let result = response.into_json_deserialize::<DatamartResponse>();
            match result {
                Ok(j) => { j },
                Err(_) => { 
                    return Err(String::from(format!("Response from datamart server is not valid JSON, or the structure has changed significantly. Target url: {}", target_url)));
                }
            }
        };

        for entry in parsed.results {
            let lookup = &config[&slug_id].independent;
            let independent = {
                match entry[lookup].as_ref() {
                    Some(value) => { value },
                    None => {
                        // FYI: this actually happens. Values with no assigned date, floating around in the response.
                        eprintln!("SLUGID={} Response contains entries with a null independent field, which is irrational. These entries will be skipped.", slug_id);
                        continue;
                    }
                }
            };

            lazy_static!{
                static ref RE_DATAMART_DATE_CAPTURE: Regex = Regex::new(r"(?P<month>\d+)/(?P<day>\d+)/(?P<year>\d+)").unwrap();
            }

            let independent = {
                match RE_DATAMART_DATE_CAPTURE.captures(&independent) {
                    Some(x) => {
                        NaiveDate::from_ymd(
                            x.name("year").unwrap().as_str().parse::<i32>().unwrap(),
                            x.name("month").unwrap().as_str().parse::<u32>().unwrap(),
                            x.name("day").unwrap().as_str().parse::<u32>().unwrap()
                        )                        
                    },
                    None => {
                        return Err(String::from(format!("Failed to parse independent column from datamart response: {}", independent)))
                    }
                }
            };

            let mut data = USDADataPackageSection::new(independent);

            for column in &config[&slug_id].sections[section].fields {
                let value = { 
                    match &entry[column] {
                        Some(s) => { String::from(s) },
                        None => { String::from("") }
                    }
                };
                data.entries.insert(String::from(column), value);
            }

            for column in &config[&slug_id].sections[section].independent {
                let value = entry.get(column).unwrap().as_ref().unwrap();
                data.independent.push(String::from(value));
            }

            section_data.push(data);
        }
    }

    Ok(result)
}