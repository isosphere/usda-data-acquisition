use crate::common::{USDADataPackage, USDADataPackageSection}; // used to emulate datamart structure for easy integration

use chrono::NaiveDate;
use regex::Regex;

fn find_line(text_array: &Vec<&str>, pattern:&Regex) -> Result<usize, String> {
    for line in 0 .. text_array.len() {
        if pattern.is_match(text_array[line]) {
            return Ok(line)
        }
    }

    return Err(String::from("No match found"))
}


pub fn lmxb463_text_parse(text: String) -> Result<USDADataPackage, String> {
    let text_array = text.split_terminator("\n").collect();

    let location: usize = {
        lazy_static! {
            static ref RE_DATE_LINE: Regex = Regex::new("^For Week Ending:").unwrap();
        }
        find_line(&text_array, &RE_DATE_LINE)?
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
                return Err(String::from("Failed to parse date line for report, aborting."));
            }
        }
    };

    let location = {
        lazy_static! {
            static ref RE_TOTAL_LOADS_A: Regex = Regex::new(r"^TOTAL LOADS OF PRODUCT REPORTED").unwrap();
            static ref RE_TOTAL_LOADS_B: Regex = Regex::new(r"^TOTAL LOADS").unwrap(); // different report version. prefer not to use as default.
        }
        
        match find_line(&text_array, &RE_TOTAL_LOADS_A) {
            Ok(line) => {
                line
            },
            Err(_) => {
                find_line(&text_array, &RE_TOTAL_LOADS_B)?
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
                return Err(String::from("Failed to capture total loads for report, aborting."));
            }
        }
    };

    let mut structure = USDADataPackage::new(String::from("LM_XB463"));
    let mut summary_section = USDADataPackageSection::new(report_date);
    summary_section.independent.push(report_date.format("%Y-%m-%d").to_string());
    
    summary_section.entries.insert(String::from("total_loads"), total_loads);
    
    // primal cutout values
    let location = {
        lazy_static! {
            static ref RE_LOCATION_CUTOUT: Regex = Regex::new(r"^Weekly Cutout Value").unwrap();
        }

        find_line(&text_array, &RE_LOCATION_CUTOUT)?
    };

    lazy_static! {
        static ref RE_PRIMAL_VALUE: Regex = Regex::new(r"(?i)(?P<label>(([A-Z])\s?)+)\s+(?P<comprehensive>\d+\.\d{2})\s+(?P<prime>\d+\.\d{2})\s+(?P<branded>\d+\.\d{2})\s+(?P<choice>\d+\.\d{2})\s+(?P<select>\d+\.\d{2})\s+(?P<ungraded>\d+\.\d{2})").unwrap();
    }    
    for i in location..=location+8 {
        match RE_PRIMAL_VALUE.captures(text_array[i]) {
            Some(x) => {
                for column in vec!["comprehensive", "prime", "branded", "choice", "select", "ungraded"] {
                    let label = x.name("label").unwrap().as_str().to_lowercase().trim().replace(" ", "_");

                    summary_section.entries.insert(
                        format!("{}__{}", label, column),
                        String::from(x.name(column).unwrap().as_str())
                    );
                }
            },
            None => { continue }
        }
    }

    let section = structure.sections.entry(String::from("summary")).or_insert(Vec::new());
    section.push(summary_section);

    // quality breakdown   
    let mut quality_section = USDADataPackageSection::new(report_date);
    quality_section.independent.push(report_date.format("%Y-%m-%d").to_string());

    let location = {
        lazy_static! {
            static ref RE_LOCATION_QUALITY_A: Regex = Regex::new(r"^Quality breakdown:").unwrap();
            static ref RE_LOCATION_QUALITY_B: Regex = Regex::new(r"^TOTAL LOADS").unwrap(); // different report version
        }

        match find_line(&text_array, &RE_LOCATION_QUALITY_A) {
            Ok(line) => { line },
            Err(_) => {
                find_line(&text_array, &RE_LOCATION_QUALITY_B)?
            }
        }
    } + 1;

    lazy_static! {
        static ref RE_QUALITY_VALUE: Regex = Regex::new(r"(?i)(?P<label>[A-Z]+)\**\s+(?P<value>([0-9,]+))").unwrap();
    }

    for i in location..=location+4 {
        let quality = RE_QUALITY_VALUE.captures(text_array[i]).unwrap();
        quality_section.entries.insert(String::from(quality.name("label").unwrap().as_str()), String::from(quality.name("value").unwrap().as_str()));
    }

    let section = structure.sections.entry(String::from("quality")).or_insert(Vec::new());
    section.push(quality_section);

    // sales type
    let mut sales_section = USDADataPackageSection::new(report_date);
    sales_section.independent.push(report_date.format("%Y-%m-%d").to_string());

    let location = {
        lazy_static! {
            static ref RE_LOCATION_SALES: Regex = Regex::new(r"(?i)^((Sales type breakdown:)|(TYPE OF SALES))").unwrap();
        }

        find_line(&text_array, &RE_LOCATION_SALES)?
    } + 1;

    lazy_static! {
        static ref RE_SALES_VALUE: Regex = Regex::new(r"(?i)(?P<label>(([A-Z0-9/\-]+)\s{0,2})+)\s+(?P<value>([0-9,]+))").unwrap();
    }

    for i in location..=location+3 {
        let sales = RE_SALES_VALUE.captures(text_array[i]).unwrap();
        sales_section.entries.insert(String::from(sales.name("label").unwrap().as_str().trim()), String::from(sales.name("value").unwrap().as_str()));
    }

    let section = structure.sections.entry(String::from("sales_type")).or_insert(Vec::new());
    section.push(sales_section);

    // destination
    let location = {
        lazy_static! {
            static ref RE_LOCATION_DESTINATION: Regex = Regex::new(r"(?i)^Destination breakdown:").unwrap();
        }
        find_line(&text_array, &RE_LOCATION_DESTINATION)
    };

    match location {
        Ok(line) => {
            let line = line + 1;

            let mut destination_section = USDADataPackageSection::new(report_date);
            destination_section.independent.push(report_date.format("%Y-%m-%d").to_string());            

            lazy_static! {
                static ref RE_DESTINATION_VALUE: Regex = Regex::new(r"(?i)(?P<label>(([A-Z]+)\s?)+)\s+(?P<value>([0-9,]+))").unwrap();
            }

            for i in line..=line+2 {
                let result = RE_DESTINATION_VALUE.captures(text_array[i]).unwrap();
                destination_section.entries.insert(String::from(result.name("label").unwrap().as_str().trim()), String::from(result.name("value").unwrap().as_str()));
            }
            
            let section = structure.sections.entry(String::from("destination")).or_insert(Vec::new());
            section.push(destination_section);            
        },
        Err(_) => {}
    }

    // delivery period
    let location = {
        lazy_static! {
            static ref RE_LOCATION_DELIVERY: Regex = Regex::new(r"(?i)^Delivery period breakdown:").unwrap();
        }
        find_line(&text_array, &RE_LOCATION_DELIVERY)
    };

    match location {
        Ok(line) => {
            let line = line + 1;

            lazy_static! {
                static ref RE_DELIVERY_VALUE: Regex = Regex::new(r"(?i)(?P<label>(([A-Z0-9-]+)\s?)+)\s+(?P<value>([0-9,]+))").unwrap();
            }

            let mut delivery_section = USDADataPackageSection::new(report_date);
            delivery_section.independent.push(report_date.format("%Y-%m-%d").to_string());            

            for i in line..=line+3 {
                let result = RE_DELIVERY_VALUE.captures(text_array[i]).unwrap();
                delivery_section.entries.insert(String::from(result.name("label").unwrap().as_str().trim()), String::from(result.name("value").unwrap().as_str()));
            }

            let section = structure.sections.entry(String::from("delivery")).or_insert(Vec::new());
            section.push(delivery_section);        
        },
        Err(_) => {}
    }

    Ok(structure)
}

pub fn dcgr110_text_parse(text: String) -> Result<USDADataPackage, String> {
    let text_array = text.split_terminator("\n").collect();

    let mut structure = USDADataPackage::new(String::from("DC_GR110"));  

    let location: usize = {
        lazy_static! {
            static ref RE_DATE_LINE: Regex = Regex::new(r"^Dodge City,\s+KS").unwrap();
        }
        find_line(&text_array, &RE_DATE_LINE)?
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
                    _ => return Err(String::from(format!("Invalid month name captured: {}",  month_name)))
                };

                NaiveDate::from_ymd(
                    x.name("year").unwrap().as_str().parse::<i32>().unwrap(),
                    month,
                    x.name("day").unwrap().as_str().parse::<u32>().unwrap()
                )
            },
            None => {
                return Err(String::from("Failed to parse date line for report, aborting."));
            }
        }
    };

    let mut location: usize = {
        lazy_static! {
            static ref RE_WHEAT_LINE: Regex = Regex::new(r"HRW WHEAT ORD US NO 1").unwrap();
        }
        find_line(&text_array, &RE_WHEAT_LINE)?
    } + 2;

    lazy_static! {
        static ref RE_PRICE_LINE: Regex = Regex::new(r"(?i)^(?P<region>(([a-z]+)\s?)+)\s+(?P<left_bid>\d+\.\d+)(?P<right_bid>\d+\.\d+)?").unwrap();
    }

    let mut section_order = vec!["soybeans", "sorghum", "corn", "wheat",];
    let mut section = structure.sections.entry(section_order.pop().unwrap().to_string()).or_insert(Vec::new());

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
                current_object.independent.push(String::from(r.name("region").unwrap().as_str().trim()));
                current_object.entries.insert(String::from("bid"), format!("{}", insert_price));
            },
            None => {
                if section_order.len() == 0 {
                    break;
                } else {
                    section = structure.sections.entry(section_order.pop().unwrap().to_string()).or_insert(Vec::new());
                    location += 2;
                }
            }
        }
        
        location += 1;

        if location == text_array.len() && section_order.len() > 0 {
            return Err(String::from(format!("Failed to parse report, hit end of report early. Missed sections: {:?}", section_order)))
        }
    }

    Ok(structure)
}