use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

#[macro_use] 
extern crate lazy_static;
extern crate toml;
extern crate serde;
extern crate ureq;

use clap::{Arg, App};
use chrono::{NaiveDate};
use postgres::{Config, NoTls};
use postgres::types::Type;
use regex::{Regex};
use rpassword::prompt_password_stdout;
use serde_derive::Deserialize;
use walkdir::{WalkDir, DirEntry};

const HTTP_CONNECT_TIMEOUT: u64 = 25000;
const HTTP_RECEIVE_TIMEOUT: u64 = 25000;

fn command_usage<'a, 'b>() -> App<'a, 'b> {
    const DEFAULT_HOST: &str = "localhost";
    const DEFAULT_PORT: &str = "5432";
    const DEFAULT_USER: &str = "postgres";

    App::new("data-acquisition")
    .author("Matthew Scheffel <matt@dataheck.com>")
    .about("Scrapes data from the USDA")
    .arg(
        Arg::with_name("backfill-text")
            .short("t")
            .long("backfill-text")
            .takes_value(true)
            .help("Trigger parsing of all files in a given directory containing historical text files for non-datamart reports")
    )
    .arg(
        Arg::with_name("backfill-datamart")
            .short("m")
            .long("backfill-datamart")
            .takes_value(false)
            .help("Trigger total download of all known datamart reports")
            .required(false)
    )
    .arg(
        Arg::with_name("datamart-config")
            .takes_value(true)
            .help("Location of datamart scraping configuration")
            .default_value("config/datamart.toml")
    )
    .arg(
        Arg::with_name("create")
            .short("c")
            .long("create")
            .takes_value(false)
            .help("Create table structure required for insertion")
    )
    .arg(
        Arg::with_name("host")
            .short("h")
            .long("host")
            .takes_value(true)
            .default_value(DEFAULT_HOST)
            .help("The hostname of the PostgreSQL server to connect to.")
    )
    .arg(
        Arg::with_name("database")
            .short("b")
            .long("database")
            .takes_value(true)
            .required(true)
            .help("The database to USE on the PostgreSQL server.")
    )
    .arg(
        Arg::with_name("port")
            .short("p")
            .long("port")
            .takes_value(true)
            .default_value(DEFAULT_PORT)
            .help("The port to connect to the PostgreSQL server on.")
    )
    .arg(
        Arg::with_name("user")
            .short("u")
            .long("user")
            .takes_value(true)
            .default_value(DEFAULT_USER)
            .help("The user to connect to the PostgreSQL server with.")
    )         
}

#[derive(Deserialize, Debug)]
struct DatamartConfig {
    name: String,                             // historical "slug name"
    description: String,
    independent: String,                      // the independent variable, i.e.: primary key
    sections: HashMap<String, Vec<String>>    // each section has a name and a list of columns to scrape
}

#[derive(Deserialize, Debug)]
struct DatamartResponse { 
    reportSection: String,
    reportSections: Vec<String>,
    stats: HashMap<String, u32>,
    results: Vec<HashMap<String, Option<String>>>
}

#[derive(Debug)]
struct USDADataPackage {
    sections: HashMap<
        String, // section name
        HashMap<
            NaiveDate, // report date
            Vec<
                HashMap<String, String> // variable name, value
            >
        >
    >,
    name: String
}

impl USDADataPackage {
    fn new(name: String) -> USDADataPackage {
        USDADataPackage {
            sections: HashMap::new(),
            name: name
        }
    }
}

fn create_table(name:String, client: &mut postgres::Client) -> Result<usize, postgres::Error> {
    client.batch_execute(&format!(r#"
        CREATE TABLE {0} (
            "timestamp" timestamp with time zone not null, 
            variable_name text not null,
            value numeric,
            value_text text,
            constraint {0}_timestamp_variable_name_pkey primary key ("timestamp", variable_name)
        );
    "#, &name))?;
    Ok(0)
}

fn prepare_client(host: Arc<String>, port: Arc<u16>, user: Arc<String>, dbname: Arc<String>, password: Arc<String>) -> postgres::Client {
    let client = Config::new()
        .host(&host)
        .port(*port)
        .user(&user)
        .dbname(&dbname)
        .password(password.to_string())
        .connect(NoTls).unwrap();

    client
}

fn insert_package(package: USDADataPackage, client: &mut postgres::Client) -> Result<usize, postgres::Error> {
    let report_name = package.name;

    for (section, dates) in package.sections {
        let table_name = String::from(format!("{}_{}", report_name, section));

        // it would be nice to avoid recontructing these on subsequent calls, but we will probably only call once per report?
        let statement = client.prepare_typed(&format!(r#"
            INSERT INTO {table_name} ("timestamp", variable_name, value) VALUES(
                TO_TIMESTAMP($1, 'YYYY-MM-DD'), $2, CAST($3 AS numeric)
            ) ON CONFLICT ON CONSTRAINT {table_name}_timestamp_variable_name_pkey DO NOTHING
            "#, table_name=&table_name),
            &[Type::TEXT, Type::TEXT, Type::TEXT] 
        ).unwrap();
        
        for (report_date, entries) in dates {
            let sql_date = report_date.format("%Y-%m-%d").to_string();

            for entry in entries {
                for (key, value) in entry {
                    let value = value.replace(",", "");
                    if value.len() > 0 {
                        client.execute(&statement, &[&sql_date, &key, &value]).unwrap();
                    }
                }
            }
        }
    }
    Ok(0)
}


fn find_line(text_array: &Vec<&str>, pattern:&Regex) -> Result<usize, String> {
    for line in 0 .. text_array.len() {
        if pattern.is_match(text_array[line]) {
            return Ok(line)
        }
    }

    return Err(String::from("No match found"))
}

fn process_datamart(slug_id: String, report_date:Option<NaiveDate>, config: &HashMap<String, DatamartConfig>) -> Result<USDADataPackage, String> {
    if !config.contains_key(&slug_id) {
        return Err(String::from(format!("Slug ID {} is not known to our datamart configuration.", slug_id)));
    }

    let report_label = &config.get(&slug_id).unwrap().name;
    let mut result = USDADataPackage::new(String::from(report_label));

    for section in config[&slug_id].sections.keys() {
        let section_data = result.sections.entry(String::from(section)).or_insert(HashMap::new());

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
                    format!("{base_url}/{section}", base_url=base_url, section=section)
                }
            }
        };

        let response = ureq::get(&target_url).timeout_connect(HTTP_CONNECT_TIMEOUT).timeout_read(HTTP_RECEIVE_TIMEOUT).call();
        
        if !response.ok() {
            return Err(String::from(format!("Failed to retrieve data from datamart server with URL {}. Error: {}", target_url, response.into_string().unwrap())));
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
            let mut data = HashMap::new();
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

            for column in &config[&slug_id].sections[section] {
                let value = { 
                    match &entry[column] {
                        Some(s) => { String::from(s) },
                        None => { String::from("") }
                    }
                };
                data.insert(String::from(column), value);
            }

            let push_target = section_data.entry(independent).or_insert(Vec::new());
            push_target.push(data);
        }
    }

    Ok(result)
}

fn lmxb463_text_parse(text: String) -> Result<USDADataPackage, String> {
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
    let mut summary_data = HashMap::new();
    summary_data.insert(String::from("total_loads"), total_loads);
    
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

                    summary_data.insert(
                        format!("{}__{}", label, column),
                        String::from(x.name(column).unwrap().as_str())
                    );
                }
            },
            None => { continue }
        }
    }

    let mut summary_report = HashMap::new();
    summary_report.insert(report_date, vec![summary_data]);
    structure.sections.insert(String::from("summary"), summary_report);    

    // quality breakdown   
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

    let mut quality_data = HashMap::new();
    for i in location..=location+4 {
        let quality = RE_QUALITY_VALUE.captures(text_array[i]).unwrap();
        quality_data.insert(String::from(quality.name("label").unwrap().as_str()), String::from(quality.name("value").unwrap().as_str()));
    }

    let mut quality_report = HashMap::new();
    quality_report.insert(report_date, vec![quality_data]);
    structure.sections.insert(String::from("quality"), quality_report);    

    // sales type
    let location = {
        lazy_static! {
            static ref RE_LOCATION_SALES: Regex = Regex::new(r"(?i)^((Sales type breakdown:)|(TYPE OF SALES))").unwrap();
        }

        find_line(&text_array, &RE_LOCATION_SALES)?
    } + 1;

    lazy_static! {
        static ref RE_SALES_VALUE: Regex = Regex::new(r"(?i)(?P<label>(([A-Z0-9/\-]+)\s{0,2})+)\s+(?P<value>([0-9,]+))").unwrap();
    }

    let mut sales_data = HashMap::new();
    for i in location..=location+3 {
        let sales = RE_SALES_VALUE.captures(text_array[i]).unwrap();
        sales_data.insert(String::from(sales.name("label").unwrap().as_str().trim()), String::from(sales.name("value").unwrap().as_str()));
    }

    let mut sales_report = HashMap::new();
    sales_report.insert(report_date, vec![sales_data]);
    structure.sections.insert(String::from("sales_type"), sales_report);    

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

            lazy_static! {
                static ref RE_DESTINATION_VALUE: Regex = Regex::new(r"(?i)(?P<label>(([A-Z]+)\s?)+)\s+(?P<value>([0-9,]+))").unwrap();
            }

            let mut destination_data = HashMap::new();

            for i in line..=line+2 {
                let result = RE_DESTINATION_VALUE.captures(text_array[i]).unwrap();
                destination_data.insert(String::from(result.name("label").unwrap().as_str().trim()), String::from(result.name("value").unwrap().as_str()));
            }
            
            let mut destination_report = HashMap::new();
            destination_report.insert(report_date, vec![destination_data]);
            structure.sections.insert(String::from("destination"), destination_report);
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

            let mut delivery_data = HashMap::new();

            for i in line..=line+3 {
                let result = RE_DELIVERY_VALUE.captures(text_array[i]).unwrap();
                delivery_data.insert(String::from(result.name("label").unwrap().as_str().trim()), String::from(result.name("value").unwrap().as_str()));
            }

            let mut delivery_report = HashMap::new();
            delivery_report.insert(report_date, vec![delivery_data]);
            structure.sections.insert(String::from("delivery"), delivery_report);            
        },
        Err(_) => {}
    }

    Ok(structure)
}

fn report_filter(entry: &DirEntry) -> bool {
    let is_folder = entry.file_type().is_dir();
    let file_name = entry.file_name().to_str().unwrap();
    let lowercase_file_name = file_name.to_lowercase();
    let file_ext = lowercase_file_name.split('.').last();

    match file_ext {
        Some(ext) => {
            ext == "txt" || is_folder
        },
        None => {
            false
        },
    }
}

fn main() {
    let matches = command_usage().get_matches();
    
    let datamart_config: HashMap<String, DatamartConfig> = toml::from_str(&fs::read_to_string(matches.value_of("datamart-config").unwrap())
        .expect("Failed to read datamart config from filesystem"))
        .expect("Failed to parse datamart config TOML");

    let postgresql_host = Arc::new(matches.value_of("host").unwrap().to_string());
    let postgresql_user = Arc::new(matches.value_of("user").unwrap().to_string());
    let postgresql_dbname = Arc::new(matches.value_of("database").unwrap().to_string());
    let postgresql_port = Arc::new(matches.value_of("port").unwrap().parse::<u16>().expect(&format!("Invalid port specified: '{}.'", matches.value_of("port").unwrap())));
    
    println!("Connecting to PostgreSQL {}:{} as user '{}'.", postgresql_host, postgresql_port, postgresql_user);
    let postgresql_pass = Arc::new(prompt_password_stdout("Password: ").unwrap());

    let mut client = prepare_client(
        postgresql_host, 
        postgresql_port, 
        postgresql_user, 
        postgresql_dbname, 
        postgresql_pass
    );

    if matches.is_present("create") {
        println!("Creating tables.");

        for section in vec!["summary", "quality", "sales_type", "destination", "delivery"] {
            create_table(String::from(format!("lm_xb463_{}", section)), &mut client).unwrap();
        }
        
        for slug in datamart_config.keys() {
            let report_name = &datamart_config.get(slug).unwrap().name;

            for section in datamart_config.get(slug).unwrap().sections.keys() {
                create_table(String::from(format!("{}_{}", report_name, section)), &mut client).unwrap();
            }
        }
    } 

    if matches.is_present("backfill-text") {
        let target_path = matches.value_of("backfill-text").unwrap();
        let mut file_queue = Vec::new();
        for entry in WalkDir::new(target_path).into_iter().filter_entry(|e| report_filter(e)) {
            match entry {
                Ok(e) => {
                    if e.file_type().is_file() {
                        file_queue.push(String::from(e.path().to_str().unwrap()))
                    } else {
                        continue; // no message required for skipping folders
                    }
                },
                Err(e) => {
                    println!("Forced to skip entry: {}", e); // file system error?
                    continue;
                }
            };  
        }
        
        for path in file_queue {
            let report = fs::read_to_string(&path).unwrap();
            let result = lmxb463_text_parse(report);

            match result {
                Ok(structure) => {
                    insert_package(structure, &mut client).unwrap();
                },
                Err(_) => {
                    eprintln!("Failed to process file: {}", &path);
                }
            }
        }
    }

    if matches.is_present("backfill-datamart") {
        for slug in datamart_config.keys() {
            let result = process_datamart(String::from(slug), None, &datamart_config);

            match result {
                Ok(structure) => {
                    insert_package(structure, &mut client).unwrap();
                },
                Err(e) => {
                    eprintln!("Failed to process datamart reponse: {}", e);
                }
            }
        }
    }
}
