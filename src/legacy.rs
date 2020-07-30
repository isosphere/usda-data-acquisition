use crate::common::{USDADataPackage, USDADataPackageSection}; // used to emulate datamart structure for easy integration

use chrono::NaiveDate;
use regex::Regex;

/// Finds the zero-indexed line number that matches a regex pattern.
/// If your regex is trivial, consider using the faster `find_line_contains`
fn find_line_regex(text_array: &[&str], pattern:&Regex) -> Option<usize> {
    for (number, line) in text_array.iter().enumerate() {
        if pattern.is_match(line) {
            return Some(number)
        }
    }

    None
}

/// Finds the zero-indexed line number that contains a string.
/// For more advanced finding, consider using the slower `find_line_regex`
fn find_line_contains(text_array: &[&str], pattern:&str) -> Option<usize> {
    for (number, line) in text_array.iter().enumerate() {
        if line.contains(pattern) {
            return Some(number)
        }
    }

    None
}

/// Finds the zero-indexed line number that starts with a string.
/// For more advanced finding, consider using the slower `find_line_regex`
/// For basic finding that isn't anchored to the start of a line, consider `find_line_contains`
fn find_line_starts_with(text_array: &[&str], pattern:&str) -> Option<usize> {
    for (number, line) in text_array.iter().enumerate() {
        if line.starts_with(pattern) {
            return Some(number)
        }
    }

    None
}

pub fn lmxb463_text_parse(text: String) -> Result<USDADataPackage, String> {
    let text_array: Vec<&str> = text.split_terminator('\n').collect();

    let location: usize = {
        match find_line_starts_with(&text_array, "For Week Ending:") {
            Some(line) => { line },
            None => {
                return Err("Failed to find date line".to_owned());
            }
        }
    };

    let report_date = {
        lazy_static! {
            static ref RE_DATE_PARSE: Regex = Regex::new(r"(?P<month>\d+)/(?P<day>\d+)/(?P<year>\d{4})").unwrap();
        }

        match RE_DATE_PARSE.captures(text_array[location]) {
            Some(x) => {
                NaiveDate::from_ymd(
                    x.name("year").unwrap().as_str().parse::<i32>().unwrap(),
                    x.name("month").unwrap().as_str().parse::<u32>().unwrap(),
                    x.name("day").unwrap().as_str().parse::<u32>().unwrap()
                )
            },
            None => {
                return Err("Failed to parse date line for report, aborting.".to_owned());
            }
        }
    };

    let location = {
        match find_line_starts_with(&text_array, "TOTAL LOADS OF PRODUCT REPORTED") {
            Some(line) => {
                line
            },
            None => {
                match find_line_starts_with(&text_array, "TOTAL LOADS") {
                    Some(line) => {line},
                    None => {
                        return Err("Failed to find total load count location.".to_owned());
                    }
                }
            }
        }
    };

    let total_loads = {
        lazy_static! {
            static ref RE_TOTAL_LOADS_CAPTURE: Regex = Regex::new(r"([0-9,]+)").unwrap();
        }

        match RE_TOTAL_LOADS_CAPTURE.captures(text_array[location]) {
            Some(x) => {
                String::from(&x[0])
            },
            None => {
                return Err("Failed to capture total loads for report, aborting.".to_owned());
            }
        }
    };

    let mut structure = USDADataPackage::new("LM_XB463".to_owned());
    let mut summary_section = USDADataPackageSection::new(report_date);
    summary_section.independent.push(report_date.format("%Y-%m-%d").to_string());
    
    summary_section.entries.insert("total_loads".to_owned(), total_loads);
    
    // primal cutout values
    let location = {
        match find_line_starts_with(&text_array, "Weekly Cutout Value") {
            Some(line) => {line},
            None => {
                return Err("Failed to locate cutout value line".to_owned());
            }
        }
    };

    lazy_static! {
        static ref RE_PRIMAL_VALUE: Regex = Regex::new(r"(?i)(?P<label>(([A-Z])\s?)+)\s+(?P<comprehensive>\d+\.\d{2})\s+(?P<prime>\d+\.\d{2})\s+(?P<branded>\d+\.\d{2})\s+(?P<choice>\d+\.\d{2})\s+(?P<select>\d+\.\d{2})\s+(?P<ungraded>\d+\.\d{2})").unwrap();
    }
    
    for line in &text_array[location..=location+8] {
        match RE_PRIMAL_VALUE.captures(line) {
            Some(x) => {
                for column in &["comprehensive", "prime", "branded", "choice", "select", "ungraded"] {
                    let label = x.name("label").unwrap().as_str().to_lowercase().trim().replace(" ", "_");

                    summary_section.entries.insert(
                        format!("{}__{}", label, column),
                        x.name(column).unwrap().as_str().to_owned()
                    );
                }
            },
            None => { continue }
        }
    }

    let section = structure.sections.entry("summary".to_owned()).or_insert_with(Vec::new);
    section.push(summary_section);

    // quality breakdown   
    let mut quality_section = USDADataPackageSection::new(report_date);
    quality_section.independent.push(report_date.format("%Y-%m-%d").to_string());

    let location = {
        match find_line_starts_with(&text_array, "Quality breakdown:") {
            Some(line) => { line },
            None => {
                // different report version
                match find_line_starts_with(&text_array, "TOTAL LOADS") {
                    Some(line) => { line },
                    None => {
                        return Err("Failed to locate quality section location".to_owned());
                    }
                }
            }
        }
    } + 1;

    lazy_static! {
        static ref RE_QUALITY_VALUE: Regex = Regex::new(r"(?i)(?P<label>[A-Z]+)\**\s+(?P<value>([0-9,]+))").unwrap();
    }

    for line in &text_array[location..=location+4] {
        let quality = RE_QUALITY_VALUE.captures(line).unwrap();
        quality_section.entries.insert(quality.name("label").unwrap().as_str().to_owned(), quality.name("value").unwrap().as_str().to_owned());
    }

    let section = structure.sections.entry("quality".to_owned()).or_insert_with(Vec::new);
    section.push(quality_section);

    // sales type
    let mut sales_section = USDADataPackageSection::new(report_date);
    sales_section.independent.push(report_date.format("%Y-%m-%d").to_string());

    let location = {
        lazy_static! {
            static ref RE_LOCATION_SALES: Regex = Regex::new(r"(?i)^((Sales type breakdown:)|(TYPE OF SALES))").unwrap();
        }

        match find_line_regex(&text_array, &RE_LOCATION_SALES) {
            Some(line) => { line },
            None => {
                return Err("Failed to locate sales section".to_owned());
            }
        }
    } + 1;

    lazy_static! {
        static ref RE_SALES_VALUE: Regex = Regex::new(r"(?i)(?P<label>(([A-Z0-9/\-]+)\s{0,2})+)\s+(?P<value>([0-9,]+))").unwrap();
    }

    for line in &text_array[location..=location+3] {
        let sales = RE_SALES_VALUE.captures(line).unwrap();
        sales_section.entries.insert(sales.name("label").unwrap().as_str().trim().to_owned(), sales.name("value").unwrap().as_str().to_owned());
    }

    let section = structure.sections.entry("sales_type".to_owned()).or_insert_with(Vec::new);
    section.push(sales_section);

    // destination
    let location = {
        lazy_static! {
            static ref RE_LOCATION_DESTINATION: Regex = Regex::new(r"(?i)^Destination breakdown:").unwrap();
        }
        find_line_regex(&text_array, &RE_LOCATION_DESTINATION)
    };

    if let Some(line) = location {
        let line = line + 1;

        let mut destination_section = USDADataPackageSection::new(report_date);
        destination_section.independent.push(report_date.format("%Y-%m-%d").to_string());            

        lazy_static! {
            static ref RE_DESTINATION_VALUE: Regex = Regex::new(r"(?i)(?P<label>(([A-Z]+)\s?)+)\s+(?P<value>([0-9,]+))").unwrap();
        }

        for line in &text_array[line..=line+2] {
            let result = RE_DESTINATION_VALUE.captures(line).unwrap();
            destination_section.entries.insert(result.name("label").unwrap().as_str().trim().to_owned(), result.name("value").unwrap().as_str().to_owned());
        }
        
        let section = structure.sections.entry("destination".to_owned()).or_insert_with(Vec::new);
        section.push(destination_section);
    }

    // delivery period
    let location = {
        lazy_static! {
            static ref RE_LOCATION_DELIVERY: Regex = Regex::new(r"(?i)^Delivery period breakdown:").unwrap();
        }
        find_line_regex(&text_array, &RE_LOCATION_DELIVERY)
    };

    if let Some(line) = location {
        let line = line + 1;

        lazy_static! {
            static ref RE_DELIVERY_VALUE: Regex = Regex::new(r"(?i)(?P<label>(([A-Z0-9-]+)\s?)+)\s+(?P<value>([0-9,]+))").unwrap();
        }

        let mut delivery_section = USDADataPackageSection::new(report_date);
        delivery_section.independent.push(report_date.format("%Y-%m-%d").to_string());

        for line in &text_array[line..=line+3] {
            let result = RE_DELIVERY_VALUE.captures(line).unwrap();
            delivery_section.entries.insert(result.name("label").unwrap().as_str().trim().to_owned(), result.name("value").unwrap().as_str().to_owned());
        }

        let section = structure.sections.entry("delivery".to_owned()).or_insert_with(Vec::new);
        section.push(delivery_section);
    }

    Ok(structure)
}

pub fn dcgr110_text_parse(text: String) -> Result<USDADataPackage, String> {
    let text_array: Vec<&str> = text.split_terminator('\n').collect();

    let mut structure = USDADataPackage::new(String::from("DC_GR110"));  

    let location: usize = {
        match find_line_starts_with(&text_array, "Dodge City, KS") {
            Some(line) => {line},
            None => {
                return Err("Failed to locate report date line".to_owned());
            }
        }
    };

    let report_date = {
        lazy_static! {
            static ref RE_DATE_PARSE: Regex = Regex::new(r"(?i)(?P<month>[a-z]+)\s+(?P<day>\d+),\s+(?P<year>\d{4})").unwrap();
        }

        match RE_DATE_PARSE.captures(text_array[location]) {
            Some(x) => {
                let month_name = x.name("month").unwrap().as_str().to_lowercase();
                let month = match month_name.as_ref() {
                    "jan" => {1},  "feb" => {2},  "mar" => {3},
                    "apr" => {4},  "may" => {5},  "jun" => {6},
                    "jul" => {7},  "aug" => {8},  "sep" => {9},
                    "oct" => {10}, "nov" => {11}, "dec" => {12},
                    _ => return Err(format!("Invalid month name captured: {}",  month_name))
                };

                NaiveDate::from_ymd(
                    x.name("year").unwrap().as_str().parse::<i32>().unwrap(),
                    month,
                    x.name("day").unwrap().as_str().parse::<u32>().unwrap()
                )
            },
            None => {
                return Err("Failed to parse date line for report, aborting.".to_owned());
            }
        }
    };

    let mut location: usize = {
        match find_line_contains(&text_array, "HRW WHEAT ORD US NO 1") {
            Some(line) => { line },
            None => {
                return Err("Failed to locate wheat line".to_owned());
            }
        }
    } + 2;

    lazy_static! {
        static ref RE_PRICE_LINE: Regex = Regex::new(r"(?i)^(?P<region>(([a-z]+)\s?)+)\s+(?P<left_bid>\d+\.\d+)(?P<right_bid>\d+\.\d+)?").unwrap();
    }

    let mut section_order = vec!["soybeans", "sorghum", "corn", "wheat",];
    let mut section = structure.sections.entry(section_order.pop().unwrap().to_string()).or_insert_with(Vec::new);

    loop {
        let result = RE_PRICE_LINE.captures(text_array[location]);

        match result {
            Some(r) => {
                let right_price = r.name("right_bid");
                let left_price = r.name("left_bid").unwrap().as_str().parse::<f32>().unwrap();

                let insert_price = {
                    match right_price {
                        Some(v) => {
                            (v.as_str().parse::<f32>().unwrap() + left_price) / 2.0
                        },
                        None => { left_price }
                    }
                };

                section.push(USDADataPackageSection::new(report_date));
                
                let current_object = section.last_mut().unwrap();
                current_object.independent.push(report_date.format("%Y-%m-%d").to_string());
                current_object.independent.push(r.name("region").unwrap().as_str().trim().to_owned());
                current_object.entries.insert("bid".to_owned(), format!("{}", insert_price));
            },
            None => {
                if section_order.is_empty() {
                    break;
                } else {
                    section = structure.sections.entry(section_order.pop().unwrap().to_string()).or_insert_with(Vec::new);
                    location += 2;
                }
            }
        }
        
        location += 1;

        if location == text_array.len() && !section_order.is_empty() {
            return Err(format!("Failed to parse report, hit end of report early. Missed sections: {:?}", section_order))
        }
    }

    Ok(structure)
}