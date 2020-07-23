use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

#[macro_use] 
extern crate lazy_static;
extern crate toml;
extern crate serde;
extern crate ureq;

use clap::{Arg, App};
use chrono::NaiveDate;
use postgres::{Config, NoTls};
use postgres::types::ToSql;

use rpassword::prompt_password_stdout;
use walkdir::{WalkDir, DirEntry};

mod common;
use common::USDADataPackage;

mod datamart;
use datamart::{DatamartConfig};

mod esmis;
use esmis::fetch_releases_by_identifier;

mod legacy;

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
        Arg::with_name("secret-config")
            .takes_value(true)
            .help("Location of private configuration (passwords, api keys, etc.)")
            .default_value("config/secret.toml")
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
    .arg(
        Arg::with_name("update")
            .long("update")
            .help("Checks latest date in database and attempts to synchronize with USDA servers from that date, per report.")
    )
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

fn insert_package(package: USDADataPackage, structure: &datamart::DatamartConfig, client: &mut postgres::Client) -> Result<usize, postgres::Error> {
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
    
    let secret_config: Option<HashMap<String, HashMap<String, String>>> = {
        let secret_result = &fs::read_to_string(matches.value_of("secret-config").unwrap());
        match secret_result {
            Ok(s) => {
                Some(toml::from_str(s).expect("Secret configuration exists yet failed to process as a TOML file."))
            },
            Err(_) => {
                None
            }
        }
    };

    let postgresql_host = Arc::new(matches.value_of("host").unwrap().to_string());
    let postgresql_user = Arc::new(matches.value_of("user").unwrap().to_string());
    let postgresql_dbname = { 
        match secret_config.as_ref() {
            Some(c) => {
                if c.contains_key("postgres") && c["postgres"].contains_key("dbname") {
                    Arc::new(String::from(&c["postgres"]["dbname"]))
                } else {
                    Arc::new(matches.value_of("database").unwrap().to_string())
                }
            },
            None => {
                if matches.is_present("database") {
                    Arc::new(matches.value_of("database").unwrap().to_string())
                } else {
                    panic!("Must specify postgres dbname either by command line argument or via secret config")
                }
                
            }
        }
    };

    let postgresql_port = Arc::new(matches.value_of("port").unwrap().parse::<u16>().expect(&format!("Invalid port specified: '{}.'", matches.value_of("port").unwrap())));
    let http_connect_timeout = Arc::new(matches.value_of("http-connect-timeout").unwrap().parse::<u64>().expect(&format!("Invalid http connect timeout specified: {}", matches.value_of("http-connect-timeout").unwrap())));
    let http_receive_timeout = Arc::new(matches.value_of("http-receive-timeout").unwrap().parse::<u64>().expect(&format!("Invalid http receive timeout specified: {}", matches.value_of("http-receive-timeout").unwrap())));
    
    println!("Connecting to PostgreSQL {}:{} as user '{}'.", postgresql_host, postgresql_port, postgresql_user);
    let postgresql_pass = {
        match secret_config.as_ref() {
            Some(c) => {
                if c.contains_key("postgres") && c["postgres"].contains_key("password") {
                    Arc::new(String::from(&c["postgres"]["password"]))
                } else {
                    Arc::new(prompt_password_stdout("Password: ").unwrap())
                }
            },
            None => {
                Arc::new(prompt_password_stdout("Password: ").unwrap())
            }
        }        
    };

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
            let result = legacy::lmxb463_text_parse(report);

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

            let result = datamart::process_datamart(String::from(slug), None, &datamart_config, http_connect_timeout, http_receive_timeout, None);
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
        let result = datamart::process_datamart(String::from(slug), None, &datamart_config, http_connect_timeout, http_receive_timeout, None);
        let current_config = datamart_config.get(slug).unwrap();

        match result {
            Ok(structure) => {
                insert_package(structure, current_config, &mut client).unwrap();
            },
            Err(e) => {
                eprintln!("Failed to process datamart reponse: {}", e);
            }
        }
    } else if matches.is_present("update") {
        for slug in datamart_config.keys() {
            let http_connect_timeout = http_connect_timeout.clone();
            let http_receive_timeout = http_receive_timeout.clone();
            let current_config = datamart_config.get(slug).unwrap();

            let maximum_existing_date = {
                let mut max_date_found: Option<NaiveDate> = None;

                for section in current_config.sections.keys() {
                    let table_name = String::from(format!("{}_{}", current_config.name, section).to_lowercase());

                    let sql = format!("SELECT MAX(report_date) FROM {}", table_name);
                    let statement = client.prepare(&sql).unwrap();
                    let row = client.query_one(&statement, &[]);
                    match row {
                        Ok(v) => { 
                            let value = v.get(0);

                            match max_date_found {
                                Some(date) => {
                                    if &date < &value {
                                        max_date_found = Some(value);
                                    }
                                },
                                None => {max_date_found = Some(value);}
                            }
                        },
                        Err(_) => {
                            panic!("Failed to obtain latest data for {}, aborting.", table_name);
                        }
                    }
                }

                max_date_found.unwrap()
            };

            println!("Current maximum date for {} is {}. Requesting new data.", current_config.name, maximum_existing_date);

            let result = datamart::process_datamart(String::from(slug), None, &datamart_config, http_connect_timeout, http_receive_timeout, Some(maximum_existing_date));
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
}
