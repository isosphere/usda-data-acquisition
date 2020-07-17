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
use postgres::types::ToSql;
use regex::{Regex};
use rpassword::prompt_password_stdout;
use serde_derive::Deserialize;
use walkdir::{WalkDir, DirEntry};

fn command_usage<'a, 'b>() -> App<'a, 'b> {
    const DEFAULT_HOST: &str = "localhost";
    const DEFAULT_PORT: &str = "5432";
    const DEFAULT_USER: &str = "postgres";
    const HTTP_CONNECT_TIMEOUT: &str = "190000";
    const HTTP_RECEIVE_TIMEOUT: &str = "190000"; // datamart doesn't use compression, it's very slow

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
        Arg::with_name("legacy-config")
            .takes_value(true)
            .help("Location of legacy scraping configuration")
            .default_value("config/legacy.toml")
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
    .arg(
        Arg::with_name("slug")
            .short("s")
            .long("slug")
            .takes_value(true)
            .help("A specific datamart report to fetch")
    )
    .arg(
        Arg::with_name("http-connect-timeout")
            .long("http-connect-timeout")
            .takes_value(true)
            .default_value(HTTP_CONNECT_TIMEOUT)
            .help("HTTP connection timeout. Note that datamart does not use compression and has large response sizes.")
    )
    .arg(
        Arg::with_name("http-receive-timeout")
            .long("http-receive-timeout")
            .takes_value(true)
            .default_value(HTTP_RECEIVE_TIMEOUT)
            .help("HTTP receive timeout. Note that datamart does not use compression and has large response sizes.")
    )     
}

#[derive(Deserialize, Debug)]
struct DatamartSection {
    independent: Vec<String>, // first is always interpreted as a NaiveDate, following are text.
    fields: Vec<String>       // all will be attempted as numeric
}

#[derive(Deserialize, Debug)]
struct DatamartConfig {
    name: String,                             // historical "slug name"
    description: String,
    independent: String,                      // the independent variable, i.e.: date for query
    sections: HashMap<String, DatamartSection> 
}

#[derive(Deserialize, Debug)]
struct DatamartResponse {
    #[serde(rename(deserialize = "reportSection"))]
    report_section: String,
    #[serde(rename(deserialize = "reportSections"))]
    report_sections: Vec<String>,
    stats: HashMap<String, u32>,
    results: Vec<HashMap<String, Option<String>>>
}

// This structure represents a single "result" object from DatamartResponse.
#[derive(Debug)]
struct USDADataPackageSection {
    report_date: NaiveDate,
    independent: Vec<String>,
    entries: HashMap<String, String>
}

impl USDADataPackageSection {
    fn new(report_date: NaiveDate) -> USDADataPackageSection {
        USDADataPackageSection {
            report_date: report_date,
            independent: Vec::new(),
            entries: HashMap::new()
        }
    }
}

#[derive(Debug)]
struct USDADataPackage {
    name: String,
    sections: HashMap<
        String, // section name
        Vec<USDADataPackageSection>
    >,
}

impl USDADataPackage {
    fn new(name: String) -> USDADataPackage {
        USDADataPackage {
            name: name,
            sections: HashMap::new(),
        }
    }
}

fn create_table(name:String, independent: &Vec<String>, client: &mut postgres::Client) -> Result<usize, postgres::Error> {
    // warning: this SQL construction is sensitive magic and prone to breaking
    let mut sql = String::from(format!(r#"
        CREATE TABLE {0} (
            report_date date not null,
    "#, &name));

    for column in &independent[1..] {
        sql.push_str(&format!("\t\"{}\" text not null,", column));
    }

    sql.push_str(&format!(r#"
        variable_name text not null,
        value real,
        value_text text,
        constraint {0}_pkeys primary key (report_date,"#, &name));
    
    for column in &independent[1..] {
        sql.push_str(&format!("\"{}\",", column));
    }
    sql.pop(); // remove trailing comma

    sql.push_str(&"));");

    client.batch_execute(&sql)?;
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

fn insert_package(package: USDADataPackage, structure: &DatamartConfig, client: &mut postgres::Client) -> Result<usize, postgres::Error> {
    let report_name = package.name;

    for (section, results) in package.sections {
        // Dynamic statement preparation
        // warning: this SQL construction is sensitive magic and prone to breaking
        let table_name = String::from(format!("{}_{}", report_name, section));
        let independent = &structure.sections[&section].independent;
        let mut sql = String::from(format!(r#"INSERT INTO {table_name} (report_date, "#, table_name=&table_name));
        
        for column in &independent[1..] {
            sql.push_str(&format!("\"{}\", ", column));
        }
        sql.push_str("variable_name, value, value_text) VALUES(");
        for i in 1..=independent.len()+3 {
            sql.push_str(&format!("${},", i));
        }
        sql.pop();
        sql.push_str(&format!(") ON CONFLICT ON CONSTRAINT {table_name}_pkeys DO NOTHING", table_name=table_name));
        
        let statement = client.prepare(&sql).unwrap();
        
        // Data processing and insertion
        for usda_package in results {
            let report_date = usda_package.report_date;
            let independent = &usda_package.independent;

            for (key, value) in usda_package.entries {
                let value_numeric = {
                    let temp = value.replace(",", "");
                    match temp.parse::<f32>() {
                        Ok(v) => { Some(v) },
                        Err(_) => { None }
                    }
                };
                if value.len() > 0 {
                    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new(); // this is some kind of magic that i do not yet understand
                    
                    params.push(&report_date);
                    for column in &independent[1..] {
                        params.push(column);
                    }
                    params.push(&key);
                    params.push(&value_numeric);
                    params.push(&value);

                    client.execute(&statement, &params[..]).unwrap();
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

fn process_datamart(slug_id: String, report_date:Option<NaiveDate>, config: &HashMap<String, DatamartConfig>, http_connect_timeout:Arc<u64>, http_receive_timeout:Arc<u64>) -> Result<USDADataPackage, String> {
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
                    format!("{base_url}/{section}", base_url=base_url, section=section)
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

    let legacy_config: HashMap<String, DatamartConfig> = toml::from_str(&fs::read_to_string(matches.value_of("legacy-config").unwrap())
        .expect("Failed to read legacy config from filesystem"))
        .expect("Failed to parse legacy config TOML");

    let postgresql_host = Arc::new(matches.value_of("host").unwrap().to_string());
    let postgresql_user = Arc::new(matches.value_of("user").unwrap().to_string());
    let postgresql_dbname = Arc::new(matches.value_of("database").unwrap().to_string());
    let postgresql_port = Arc::new(matches.value_of("port").unwrap().parse::<u16>().expect(&format!("Invalid port specified: '{}.'", matches.value_of("port").unwrap())));
    let http_connect_timeout = Arc::new(matches.value_of("http-connect-timeout").unwrap().parse::<u64>().expect(&format!("Invalid http connect timeout specified: {}", matches.value_of("http-connect-timeout").unwrap())));
    let http_receive_timeout = Arc::new(matches.value_of("http-receive-timeout").unwrap().parse::<u64>().expect(&format!("Invalid http receive timeout specified: {}", matches.value_of("http-receive-timeout").unwrap())));
    
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

        let lmxb463_independent = vec![String::from("report_date")];
        for section in vec!["summary", "quality", "sales_type", "destination", "delivery"] {
            create_table(String::from(format!("lm_xb463_{}", section)), &lmxb463_independent, &mut client).unwrap();
        }
        
        for slug in datamart_config.keys() {
            let current_config = &datamart_config.get(slug).unwrap();
            let report_name = &current_config.name;

            for (section_name, section_data) in &datamart_config.get(slug).unwrap().sections {
                create_table(String::from(format!("{}_{}", report_name, section_name)), &section_data.independent, &mut client).unwrap();
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
        
        // TODO: don't assume LM_XB463
        for path in file_queue {
            let report = fs::read_to_string(&path).unwrap();
            let current_config = legacy_config.get("LM_XB463").unwrap();
            let result = lmxb463_text_parse(report);            

            match result {
                Ok(structure) => {
                    insert_package(structure, current_config, &mut client).unwrap();
                },
                Err(_) => {
                    eprintln!("Failed to process file: {}", &path);
                }
            }
        }
    }

    if matches.is_present("backfill-datamart") {
        for slug in datamart_config.keys() {
            let http_connect_timeout = http_connect_timeout.clone();
            let http_receive_timeout = http_receive_timeout.clone();

            let result = process_datamart(String::from(slug), None, &datamart_config, http_connect_timeout, http_receive_timeout);
            let current_config = datamart_config.get(slug).unwrap();

            match result {
                Ok(structure) => {
                    insert_package(structure, current_config, &mut client).unwrap();
                },
                Err(e) => {
                    eprintln!("Failed to process datamart reponse: {}", e);
                }
            }
        }
    } else if matches.is_present("slug") {
        let slug = matches.value_of("slug").unwrap();
        let result = process_datamart(String::from(slug), None, &datamart_config, http_connect_timeout, http_receive_timeout);
        let current_config = datamart_config.get(slug).unwrap();

        match result {
            Ok(structure) => {
                insert_package(structure, current_config, &mut client).unwrap();
            },
            Err(e) => {
                eprintln!("Failed to process datamart reponse: {}", e);
            }
        }        
    }


}
