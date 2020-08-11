use std::collections::HashMap;

use chrono::{NaiveDate, Local};
use serde::Deserialize;

use super::datamart::DatamartResponse;
use super::{USDADataPackage, USDADataPackageSection};

const MARS_BASE_URL: &str = "https://marsapi.ams.usda.gov/services/v1/reports";

const CONNECT_TIMEOUT: u64 = 5000;
const RECEIVE_TIMEOUT: u64 = 5000;

#[derive(Deserialize, Debug)]
pub struct ReportMetadata {
    slug_id: String,
    report_title: String,
    published_date: String,
    markets: Vec<String>,
    market_types: Vec<String>,
    offices: Vec<String>,
    #[serde(rename(deserialize = "sectionNames"))]
    section_names: Vec<String>
}

#[derive(Deserialize, Debug)]
pub struct ReportResult {
    results: Vec<HashMap<String, Option<String>>>
}

pub fn list_reports(api_key: &str) -> Result<Vec<ReportMetadata>, String> {
    let response = ureq::get(MARS_BASE_URL).set("User-Agent", super::USER_AGENT).auth(api_key, &"".to_owned()).timeout_connect(CONNECT_TIMEOUT).timeout_read(RECEIVE_TIMEOUT).call();

    if let Some(error) = response.synthetic_error() {
        return Err(format!("Failed to retrieve data from MARS server with URL {}. Error: {}", MARS_BASE_URL, error));
    }

    //println!("{:?}", response.into_string().unwrap());

    let result = response.into_json_deserialize::<Vec<ReportMetadata>>();
    match result {
        Ok(r) => { Ok(r) },
        Err(_) => { 
            Err(format!("Response from MARS server is not valid JSON, or the structure has changed significantly. Target url: {}", MARS_BASE_URL))
        }
    }
}

pub fn get_report(api_key: &str, report: &str, minimum_begin_date: Option<NaiveDate>) -> Result<(), String> {
    let target = match minimum_begin_date {
        Some(d) => {
            let today = Local::now().naive_local().date();
            format!(
                "{}/{}?report_begin_date={}:{}", MARS_BASE_URL, report,
                d.format("%Y-%m-%d"),
                today.format("%Y-%m-%d")
            )
        },
        None => {format!("{}/{}", MARS_BASE_URL, report)}
    };

    let response = ureq::get(&target).set("User-Agent", super::USER_AGENT).auth(api_key, &"".to_owned()).timeout_connect(CONNECT_TIMEOUT).timeout_read(RECEIVE_TIMEOUT).call();

    if let Some(error) = response.synthetic_error() {
        return Err(format!("Failed to retrieve data from MARS server with URL {}. Error: {}", target, error));
    }

    let result = response.into_json_deserialize::<ReportResult>();
    match result {
        Ok(r) => { println!("{:?}", r.results[0]) },
        Err(_) => { 
            return Err(format!("Response from MARS server is not valid JSON, or the structure has changed significantly. Target url: {}", target))
        }
    };

    Ok(())
}


#[test]
fn test_list_reports() {
    use std::fs;
    use std::collections::HashMap;

    let secret_config: HashMap<String, HashMap<String, String>> = {
        let secret_result = &fs::read_to_string("config/secret.toml");
        match secret_result {
            Ok(s) => {
                toml::from_str(s).expect("Secret configuration exists yet failed to process as a TOML file.")
            },
            Err(_) => {
                panic!("Need config with mars key")
            }
        }
    };

    println!("{:?}", list_reports(&secret_config["mars"]["key"]).unwrap());
}

#[test]
fn test_get_report() {
    use std::fs;
    use std::collections::HashMap;

    let secret_config: HashMap<String, HashMap<String, String>> = {
        let secret_result = &fs::read_to_string("config/secret.toml");
        match secret_result {
            Ok(s) => {
                toml::from_str(s).expect("Secret configuration exists yet failed to process as a TOML file.")
            },
            Err(_) => {
                panic!("Need config with mars key")
            }
        }
    };

    println!("{:?}", get_report(&secret_config["mars"]["key"], "1095", None).unwrap());
}