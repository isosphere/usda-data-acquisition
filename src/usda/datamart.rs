use std::collections::HashMap;
use std::sync::Arc;

use chrono::{NaiveDate, Local};
use regex::Regex;
use serde::Deserialize;

use super::{USDADataPackage, USDADataPackageSection};

#[derive(Deserialize, Debug)]
pub struct DatamartSection {
    pub alias: Option<String>,    // if present, will be used instead of hash key for table name
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
    results: Option<Vec<HashMap<String, Option<String>>>>,
    message: Option<String>
}

/// Datamart is not very reliable, and we must use very large timeouts to capture data.
/// This function does a simple query that is expected to return quickly to ensure
/// that datamart is working and ready for more serious queries, so that we can avoid our
/// long timeout.
pub fn check_datamart() -> Result<(), String> {
    const QUICK_DATAMART_TIMEOUT: u64 = 3000;

    // this is the fastest query I can find
    let target_url = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports/2451/?q=report_date=01/01/2020".to_owned();
    
    let response = ureq::get(&target_url).timeout_connect(QUICK_DATAMART_TIMEOUT).timeout_read(QUICK_DATAMART_TIMEOUT).call();
        
    if let Some(error) = response.synthetic_error() {
        return Err(format!("Failed to retrieve data from datamart server with URL {}. Error: {}", target_url, error));
    }

    let result = response.into_json_deserialize::<DatamartResponse>();
    match result {
        Ok(_) => { Ok(()) },
        Err(_) => { 
            Err(format!("Response from datamart server is not valid JSON, or the structure has changed significantly. Target url: {}", target_url))
        }
    }
}


pub fn process_datamart(slug_id: String, report_date:Option<NaiveDate>, config: &HashMap<String, DatamartConfig>, http_connect_timeout:Arc<u64>, http_receive_timeout:Arc<u64>, minimum_date:Option<NaiveDate>) -> Result<USDADataPackage, String> {
    if !config.contains_key(&slug_id) {
        return Err(format!("Slug ID {} is not known to our datamart configuration.", slug_id));
    }

    let report_label = match &config.get(&slug_id) {
        Some(v) => {&v.name},
        None => {return Err(format!("Unable to find slug ID in configuration: {}", slug_id))}
    };

    let mut result = USDADataPackage::new(report_label.to_owned());

    for section in config[&slug_id].sections.keys() {
        let section_data = result.sections.entry(section.to_owned()).or_insert_with(Vec::new);

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
            return Err(format!("Failed to retrieve data from datamart server with URL {}. Error: {}", target_url, error));
        }

        let parsed = {
            let result = response.into_json_deserialize::<DatamartResponse>();
            match result {
                Ok(j) => { j },
                Err(_) => { 
                    return Err(format!("Response from datamart server is not valid JSON, or the structure has changed significantly. Target url: {}", target_url));
                }
            }
        };

        // the +1 is a datamart oddity
        if parsed.stats["returnedRows:"] == parsed.stats["userAllowedRows:"] + 1 {
            println!("Warning: datamart response row count is max limit, there may be additional data available.");
        }

        if let Some(message) = parsed.message {
            println!("Message from datamart: {}", message)
        };

        match parsed.results {
            Some(results) => {
                'entries: for entry in results {
                    let lookup = &config[&slug_id].independent;
                    let independent = {
                        match entry[lookup].as_ref() {
                            Some(value) => { value },
                            None => {
                                // FYI: this actually happens. Values with no assigned date, floating around in the response.
                                eprintln!("slug={} Response contains entries with a null independent field, which is irrational. These entries will be skipped.", slug_id);
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
                                return Err(format!("Failed to parse independent column from datamart response: {}", independent))
                            }
                        }
                    };

                    let mut data = USDADataPackageSection::new(independent);

                    for column in &config[&slug_id].sections[section].fields {
                        let value = { 
                            match &entry[column] {
                                Some(s) => { s.to_owned() },
                                None => { "".to_owned() }
                            }
                        };
                        data.entries.insert(column.to_owned(), value);
                    }

                    for column in &config[&slug_id].sections[section].independent {
                        let value = match entry.get(column) {
                            Some(v) => {
                                match v.as_ref() {
                                    Some(v) => { v },
                                    None => {
                                        eprintln!("Failed to get value of independent column `{}` in response for date {}.", column, independent);
                                        eprintln!("This entry will be skipped. If this happens frequently, your configuration may be wrong to assume this column is an independent.");
                                        continue 'entries;
                                    }
                                }
                            }
                            None => {
                                return Err(format!("Failed to find independent column `{}` in response for date {}. All columns: {:#?}", column, independent, entry.keys()));
                            }
                        };
                        
                        data.independent.push(value.to_owned());
                    }

                    section_data.push(data);
                }
            },
            None => {
                return Err("No results found.".to_owned())
            }
        }
    }

    Ok(result)
}