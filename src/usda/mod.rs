use std::collections::HashMap;

pub mod datamart;
pub mod esmis;
pub mod legacy;
pub mod mars;

use chrono::NaiveDate;

pub const USER_AGENT: &str = "data-acquistion/0.1";

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
