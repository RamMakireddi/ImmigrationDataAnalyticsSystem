import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN=config.get('IAM_ROLE','ARN')

#CREATE SCHEMA
public_schema_create="CREATE SCHEMA IF NOT EXISTS immigration";
staging_schema_create="CREATE SCHEMA IF NOT EXISTS immigration_stage";

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS sparkify_stage.staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS sparkify_stage.staging_songs"
user_table_drop = "DROP TABLE IF EXISTS sparkify.user"
time_table_drop = "DROP TABLE IF EXISTS sparkify.time"
song_table_drop = "DROP TABLE IF EXISTS sparkify.song"
artist_table_drop = "DROP TABLE IF EXISTS sparkify.artist"
songplay_table_drop = "DROP TABLE IF EXISTS sparkify.songplays"

# CREATE STAGING TABLES
staging_immigration_table_create= ("""
    CREATE TABLE IF NOT EXISTS immigration_stage.immigration_stage (
        arrdate DATE,
        i94cit INTEGER,
        country_of_citizenship VARCHAR(50),
        i94port VARCHAR(25),
        port_of_entry CHAR(25),
        i94mode INTEGER,
        mode_of_entry VARCHAR(25),
        i94addr VARCHAR(25),
        destination_state VARCHAR(25),
        gender CHAR(1),
        visatype VARCHAR(10),
        i94visa INTEGER,
        visa_category VARCHAR(25),
        count INTEGER
        );
""")

# CREATE PUBLIC TABLES
immigration_table_create= ("""
    CREATE TABLE IF NOT EXISTS immigration.immigration (
        id BIGINT IDENTITY(0,1) PRIMARY KEY ,
        arrdate DATE NOT NULL DISTKEY,
        i94cit INTEGER NOT NULL,
        i94port VARCHAR(25) NOT NULL,
        i94mode INTEGER NOT NULL,
        i94addr VARCHAR(25) NOT NULL,
        gender CHAR(1) NOT NULL,
        visatype VARCHAR(10) NOT NULL,
        count INTEGER,
        FOREIGN KEY (i94cit) REFERENCES immigration.citizenship_country (id)
        , FOREIGN KEY (i94port) REFERENCES immigration.entry_port (id)
        , FOREIGN KEY (i94mode) REFERENCES immigration.entry_mode (id)
        , FOREIGN KEY (i94addr) REFERENCES immigration.destination_state (id)
        , FOREIGN KEY (visatype) REFERENCES immigration.visa (visatype)
        ) SORTKEY (arrdate,i94mode,visatype,i94cit,i94port,i94addr);
        );
""")

visa_table_create = ("""
    CREATE TABLE IF NOT EXISTS immigration.visa (
        visatype VARCHAR(25) NOT NULL SORTKEY PRIMARY KEY, 
        visa_category_code INTEGER, 
        visa_category VARCHAR(25) 
       ) DISTSTYLE all;
""")

"""
    INSERT INTO immigration.visa values ('missing', 999, 'missing');
    INSERT INTO immigration.citizenship_country values (999, 'missing');
    INSERT INTO immigration.destination_state values ('missing', 'missing');    
    INSERT INTO immigration.destination_state values ('missing', 'missing');
"""
     

citizenship_country_table_create = ("""
    CREATE TABLE IF NOT EXISTS immigration.citizenship_country (
        id INTEGER SORTKEY PRIMARY KEY,
        country_of_citizenship VARCHAR(25) NOT NULL
       ) DISTSTYLE all;
""")

destination_state_table_create = ("""
    CREATE TABLE IF NOT EXISTS immigration.destination_state (
        id VARCHAR(10) SORTKEY PRIMARY KEY,
        destination_state VARCHAR(25) NOT NULL
       ) DISTSTYLE all;
""")

entry_mode_table_create = ("""
    CREATE TABLE IF NOT EXISTS immigration.entry_mode (
        id INTEGER SORTKEY PRIMARY KEY,
        mode_of_entry VARCHAR(25) NOT NULL
       ) DISTSTYLE all;
""")

entry_port_table_create = ("""
    CREATE TABLE IF NOT EXISTS immigration.entry_port (
        id VARCHAR(10) SORTKEY PRIMARY KEY,
        mode_of_entry VARCHAR(25) NOT NULL
       ) DISTSTYLE all;
""")

date_table_create = ("""
    CREATE TABLE IF NOT EXISTS immigration.date (
        date DATE SORTKEY DISTKEY PRIMARY KEY, 
        day INTEGER NOT NULL, 
        week INTEGER NOT NULL, 
        month INTEGER NOT NULL, 
        year INTEGER NOT NULL, 
        weekday INTEGER NOT NULL   
        );
""")

