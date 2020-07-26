// https://usda.library.cornell.edu/apidoc/index.html#/release/findReleaseByIdentifier

use std::sync::Arc;
use chrono::NaiveDate;

use serde::Deserialize; 

#[derive(Deserialize, Debug)]
pub struct ESMISRelease {
    pub id: String,
    pub files: Vec<String>,
    pub title: Vec<String>,
    pub release_datetime: String,     // YYYY-MM-DD
    pub date_updated: Option<String>, // 
    pub date_modified: Option<String>,// these are actually ISO8601 strings including seconds and timezone
    pub identifier: Vec<String>,
    pub agency_acronym: Vec<String>,
    pub bibliographic_citation: Option<Vec<String>>,
    pub description: Option<Vec<String>>,
    pub equipment_software: Option<Vec<String>>,
    pub temporal_coverage: Option<String>
}

const API_ROOT: &str = "https://usda.library.cornell.edu/api/v1";

pub fn fetch_releases_by_identifier(token:&String, identifier:String, start_date: Option<NaiveDate>, end_date: Option<NaiveDate>, http_connect_timeout:Arc<u64>, http_receive_timeout:Arc<u64>) -> Result<Option<Vec<String>>, String> {
    let target_url = {
        let base = format!("{}/release/findByIdentifier/{}", API_ROOT, identifier);

        match (start_date, end_date) {
            (None, Some(_)) => {return Err(String::from("start_date and end_date must be specified together, or not at all."))},
            (Some(_), None) => {return Err(String::from("start_date and end_date must be specified together, or not at all."))},
            (None, None) => { base },
            (Some(start), Some(end)) => {
                format!("{}?start_date={}&end_date={}", base, start.format("%Y-%m-%d"), end.format("%Y-%m-%d"))
            }
        }
    };

    let response = ureq::get(&target_url)
        .set("Authorization", &String::from(format!("Bearer {}", token)))
        .timeout_connect(*http_connect_timeout).timeout_read(*http_receive_timeout).call();

    if let Some(error) = response.synthetic_error() {
        return Err(String::from(format!("Failed to retrieve data from datamart server with URL {}. Error: {}", target_url, error)));
    }

    let parsed = {
        let result = response.into_json_deserialize::<Vec<ESMISRelease>>();
        match result {
            Ok(j) => { j },
            Err(_) => { 
                return Err(String::from(format!("Response from datamart server is not valid JSON, or the structure has changed significantly. Target url: {}", target_url)));
            }
        }
    };

    let mut result: Vec<String> = Vec::new();

    for release in parsed {
        result.push(String::from(release.files.first().unwrap()));
    }

    Ok(Some(result))
}