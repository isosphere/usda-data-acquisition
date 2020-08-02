extern crate ftp;

use std::fmt;
use std::fmt::{Display, Formatter};
use std::io::{Read, Cursor};
use std::convert::TryInto;
use std::result;

use fixed_width::{Reader, FixedWidth, Field, LineBreak};
use flate2::read::GzDecoder;
use ftp::FtpStream;
use ftp::types::FileType::Binary;
use tar::Archive;

use serde::{Deserialize, Deserializer};
use serde::de::Error;

/*pub enum Element {
    Precipitation,  // PRCP, tenths of mm
    Snowfall,       // SNOW (mm)
    SnowDepth,      // SNWD (mm)
    MaxTemp,        // TMAX (tenths of degrees C)
    MinTemp,        // TMIN (tenths of degrees C)
    AverageCloudinessM2MCeilometer,     // ACMC, %
    AverageCloudinessM2MManual,         // ACMH, %
    AverageCloudinessS2SCeilometer,     // ACSC, %
    AverageCloudinessS2SManual,         // ACSH, %
}*/

#[derive(Debug)]
pub enum MeasurementFlag {
    PrecipitationTotalFromTwoTwelveHourTotals,
    PrecipitationTotalFromFourSixHourTotals,
    HourlyPoint, // either highest in an hour (TMIN), lowest in an hour (TMAX), or average over an hour (TAVG)
    ConvertedFromKnots,
    TemperatureLaggedFromObservation,
    ConvertedFromOktas,
    MissingPresumedZero,
    TraceOfPrecipitation, // or snow fall, or snow depth
    ConvertedFromWBANCode,
}

impl<'de> Deserialize<'de> for MeasurementFlag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
            let s = String::deserialize(deserializer)?;

            match s.as_ref() {
                "B" => {Ok(MeasurementFlag::PrecipitationTotalFromTwoTwelveHourTotals)},
                "D" => {Ok(MeasurementFlag::PrecipitationTotalFromFourSixHourTotals)},
                "H" => {Ok(MeasurementFlag::HourlyPoint)},
                "K" => {Ok(MeasurementFlag::ConvertedFromKnots)},
                "L" => {Ok(MeasurementFlag::TemperatureLaggedFromObservation)},
                "O" => {Ok(MeasurementFlag::ConvertedFromOktas)},
                "P" => {Ok(MeasurementFlag::MissingPresumedZero)},
                "T" => {Ok(MeasurementFlag::TraceOfPrecipitation)},
                "W" => {Ok(MeasurementFlag::ConvertedFromWBANCode)},
                q => {Err(D::Error::custom(format!("Unknown measurement flag: {}", q)))}
            }
        }
}

impl Display for MeasurementFlag {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            MeasurementFlag::PrecipitationTotalFromTwoTwelveHourTotals => {write!(f, "PrecipitationTotalFromTwoTwelveHourTotals")},
            MeasurementFlag::PrecipitationTotalFromFourSixHourTotals => {write!(f, "PrecipitationTotalFromFourSixHourTotals")},
            MeasurementFlag::HourlyPoint => {write!(f, "HourlyPoint")},
            MeasurementFlag::ConvertedFromKnots => {write!(f, "ConvertedFromKnots")},
            MeasurementFlag::TemperatureLaggedFromObservation => {write!(f, "TemperatureLaggedFromObservation")},
            MeasurementFlag::ConvertedFromOktas => {write!(f, "ConvertedFromOktas")},
            MeasurementFlag::MissingPresumedZero => {write!(f, "MissingPresumedZero")},
            MeasurementFlag::TraceOfPrecipitation => {write!(f, "TraceOfPrecipitation")},
            MeasurementFlag::ConvertedFromWBANCode => {write!(f, "ConvertedFromWBANCode")},
        }
    }
}


#[derive(Debug)]
pub enum QualityFlag {
    Duplicate,              // D
    Gap,                    // G
    InternalConsistency,    // I
    StreakFrequent,         // K
    Length,                 // L
    Megaconsistency,        // M
    Naught,                 // N
    ClimatologicalOutlier,  // O
    LaggedRange,            // R
    SpatialConsistency,     // S
    TemporalConsistency,    // T
    TooWarmForSnow,         // W
    FailedBoundsCheck,      // X
    FlaggedDatzilla,        // Z
}

impl<'de> Deserialize<'de> for QualityFlag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
            let s = String::deserialize(deserializer)?;

            match s.as_ref() {
                "D" => {Ok(QualityFlag::Duplicate)},
                "G" => {Ok(QualityFlag::Gap)},
                "I" => {Ok(QualityFlag::InternalConsistency)},
                "K" => {Ok(QualityFlag::StreakFrequent)},
                "L" => {Ok(QualityFlag::Length)},
                "M" => {Ok(QualityFlag::Megaconsistency)},
                "N" => {Ok(QualityFlag::Naught)},
                "O" => {Ok(QualityFlag::ClimatologicalOutlier)},
                "R" => {Ok(QualityFlag::LaggedRange)},
                "S" => {Ok(QualityFlag::SpatialConsistency)},
                "T" => {Ok(QualityFlag::TemporalConsistency)},
                "W" => {Ok(QualityFlag::TooWarmForSnow)},
                "X" => {Ok(QualityFlag::FailedBoundsCheck)},
                "Z" => {Ok(QualityFlag::FlaggedDatzilla)},
                q => {Err(D::Error::custom(format!("Unknown quality flag: {}", q)))}
            }
        }
}

impl Display for QualityFlag {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            QualityFlag::Duplicate => {write!(f, "Duplicate")},
            QualityFlag::Gap => {write!(f, "Gap")},
            QualityFlag::InternalConsistency => {write!(f, "InternalConsistency")},
            QualityFlag::StreakFrequent => {write!(f, "StreakFrequent")},
            QualityFlag::Length => {write!(f, "Length")},
            QualityFlag::Megaconsistency => {write!(f, "Megaconsistency")},
            QualityFlag::Naught => {write!(f, "Naught")},
            QualityFlag::ClimatologicalOutlier => {write!(f, "ClimatologicalOutlier")},
            QualityFlag::LaggedRange => {write!(f, "LaggedRange")},
            QualityFlag::SpatialConsistency => {write!(f, "SpatialConsistency")},
            QualityFlag::TemporalConsistency => {write!(f, "TemporalConsistency")},
            QualityFlag::TooWarmForSnow => {write!(f, "TooWarmForSnow")},
            QualityFlag::FailedBoundsCheck => {write!(f, "FailedBoundsCheck")},
            QualityFlag::FlaggedDatzilla => {write!(f, "FlaggedDatzilla")},
        }
    }
}

fn value_process<'de, D>(deserializer: D) -> Result<Option<isize>, D::Error> 
    where D: Deserializer<'de> {
    let input = isize::deserialize(deserializer)?;

    if input == -9999 {
        Ok(None)
    } else {
        Ok(Some(input))
    }
}

#[derive(Deserialize, Debug)]
pub struct DailyObservation {
    #[serde(deserialize_with = "value_process")]
    pub value: Option<isize>,
    pub measure_flag: Option<MeasurementFlag>,
    pub quality_flag: Option<QualityFlag>,
    pub source_flag: String
}

impl fmt::Display for DailyObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} (M={:?}, Q={:?}, S={:?})", self.value, self.measure_flag, self.quality_flag, self.source_flag)
    }
}


#[derive(Deserialize, Debug)]
pub struct Observation {
    pub station_id: String,
    pub year: usize,
    pub month: usize,
    pub element: String,
    pub observations: Vec<DailyObservation>
}

impl FixedWidth for Observation {
    fn fields() -> Vec<Field> {
        let mut field_vec = vec![
            Field::default().range(0..11),
            Field::default().range(11..15),
            Field::default().range(15..17),
            Field::default().range(17..21)
        ];

        let mut index = 21;
        for _ in 0..31 {
            field_vec.push(Field::default().range(index..index+5)); // value
            field_vec.push(Field::default().range(index+5..index+6)); // m flag
            field_vec.push(Field::default().range(index+6..index+7)); // q flag
            field_vec.push(Field::default().range(index+7..index+8)); // s flag
            index += 8;
        }

        field_vec
    }
}

impl fmt::Display for Observation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Station ID: '{}'. {}-{:02}. Element: {}", self.station_id, self.year, self.month, self.element)?;
        writeln!(f, "Records:")?;
        for i in 0..31 {
            let day = self.observations.get(i).unwrap();
            writeln!(f, "{} = {}", i+1, day)?;
        }
        Ok(())
    }
}

pub fn retrieve_noaa_ftp() -> Result<Cursor<Vec<u8>>, String> {
    let mut ftp_stream = {
        match FtpStream::connect("ftp.ncdc.noaa.gov:21") {
            Ok(stream) => { stream },
            Err(e) => {
                return Err(e.to_string())
            }
        }
    };

    match ftp_stream.login("anonymous", "matt@dataheck.com") {
        Ok(_) => {},
        Err(e) => {
            return Err(e.to_string())
        }
    }

    match ftp_stream.transfer_type(Binary) {
        Ok(_) => {},
        Err(e) => {
            return Err(format!("Failed to set transfer type to binary: {}", e))
        }
    }

    let cursor = { 
        match ftp_stream.simple_retr("/pub/data/ghcn/daily/ghcnd_gsn.tar.gz") {
            Ok(stream) => { stream },
            Err(e) => {
                return Err(format!("Failed to read stream: {}", e))
            }
        }
    };

    Ok(cursor)
}

pub fn process_noaa<R: Read>(cursor: R, element_filter: Option<String>, station_country_filter: Option<String>) -> Result<Vec<Observation>, String> {   
    let tar = GzDecoder::new(cursor);
    match tar.header() {
        Some(_) => {},
        None => {
            return Err(String::from("Gzip header is not valid"))
        }
    }

    let mut archive = Archive::new(tar);

    let entries = match archive.entries() {
        Ok(result) => { result },
        Err(_) => { return Err(String::from("Failed to read archive from NOAA")) }
    };

    let mut results = Vec::new();
    for file in entries {
        let mut file = match file {
            Ok(f) => {f},
            Err(_) => {return Err(String::from("Failed to read file in archive from NOAA"))}
        };

        let path_name = file.path().unwrap().into_owned().to_str().unwrap_or("Unknown").to_string();

        let file_size: usize = match file.header().size() {
            Ok(s) => {
                match s.try_into() {
                    Ok(s) => { s },
                    Err(_) => {
                        return Err(String::from("File in archive too large, unable to process."))
                    }
                }
            },
            Err(_) => {
                return Err(String::from("Failed to identify size of file in archive from NOAA"))
            }
        };

        let mut buffer = Vec::with_capacity(file_size);
        match file.read_to_end(&mut buffer) {
            Ok(_) => {},
            Err(e) => {return Err(format!("Failed to read file in archive into memory: {}, {}", path_name, e))}
        }

        let mut reader = Reader::from_bytes(buffer).width(269).linebreak(LineBreak::Newline);
        
        for row in reader.byte_reader().filter_map(result::Result::ok) {
            let record_result: Result<Observation, _> = fixed_width::from_bytes(&row);

            match record_result {
                Ok(record) => {
                    match (element_filter.as_ref(), station_country_filter.as_ref()) {
                        (Some(element), Some(country)) => {
                            if *element == record.element && record.station_id.to_lowercase().starts_with(&country.to_lowercase()) {
                                results.push(record);
                            }
                        },
                        (None, Some(country)) => {
                            if record.station_id.to_lowercase().starts_with(&country.to_lowercase()) {
                                results.push(record);
                            }
                        }
                        (Some(element), None) => {
                            if *element == record.element {
                                results.push(record);
                            }
                        }
                        (None, None) => {
                            results.push(record);
                        }
                    }
                },
                Err(e) => {
                    println!("error for {}: {}", path_name, e)
                }
            }
        }
    }

    Ok(results)
}

#[test]
fn test_process_noaa() {
    use tar::{Builder, Header};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::prelude::*;

    // note: this data is made up so that we see a variety in the response, so don't worry about weird flags
    let test_string = r#"AE000041196194403TAVG-9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999     292H S  274H S  242H S  250H S  263H S  257H S  233H S  239H S  217H S  245H S  292H S  260H S
AE000041196194404TMAX  258  I  263  I  258  I  263  I  296  I  302  I  358  I  391  I  380  I  308  I  291  I  274  I  280  I  369  I  330 KI  335B I  385  I  385  I  374  I  374  I  313  I  308  I  308  I  302  I  313  I  330  I  335  I  302  I  313  I  346  I-9999   
AE000041196194404TMIN  180  I  180  I  163  I  146  I  135  I-9999   -9999     196  I  235  I  213  I  163  I-9999     180  I  174  I-9999     196  I  241  I  235  I  208  I  196  I  208  I  213  I  180  I  174  I  180  I  180  I  169  I  152  I  169  I  169  I-9999   
"#;

    let cursor = Cursor::new(test_string);
    
    let mut header = Header::new_gnu();
    header.set_path("foo.dly").unwrap();
    header.set_size(test_string.len().try_into().unwrap());
    header.set_cksum();
    
    let mut archive = Builder::new(Vec::new());
    archive.append(&header, cursor).unwrap();
    let archive = archive.into_inner().unwrap();

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&archive[..]).unwrap();

    let result = encoder.finish().unwrap();
    let cursor = Cursor::new(result);

    let results = process_noaa(cursor, Some("TAVG".to_string()), Some("US".to_owned())).unwrap();
    for observation in results {
        println!("{}", observation);
    }
}