## Goal
    The goal of this project is the build data warehouse analytical system in AWS cloud environment in Redshift to analyze data on immigration to the United States and airport codes
    
## Source
    I94 Immigration:
        The source data for I94 immigration data is available in local disk in the format of sas7bdat. This data comes from US National Tourism and Trade Office. The data dictionary is also included in this project for reference. The actual source of the data is from https://travel.trade.gov/research/reports/i94/historical/2016.html. It is highly recommended to use it for educational purpose only but not for commercial or any other purpose. Also sample data is placed under data folder in csv format for quick exploratory data analysis (EDA)
    
    Airport Code:
        This is a simple table with airport codes. The source of this data is from https://datahub.io/core/airport-codes#data. It is highly recommended to use it for educational purpose only but not for commercial or any other purpose.
    
    Other sources:
       Other text files such as I94Addr.txt, I94CIT_I94RES.txt, I94Mode.txt, I94Port.txt and I94Visa.txt files are used to enrich immigration data for better analysis. These files are created from the I94_SAS_Labels_Descriptions.SAS file provided to describe each and every field in the immigration data.
    
## Target
    The target data warehouse will be modeled as STAR schema in Amazon Redshift for easy and effective data analysis

## Project Scope
    The scope of this project is the extract the necessary data from source and model them into star schema in Redshift for data analysis 
    
## Design Approach
   ### ELTT(Extract Load Transform Transform)
       ELTT design approach is taken to design and develop this project for effectiveness, efficiency, scalability and reusability
       -> E - Extract all the source data files from their respecive sources. No transformations are applied in this phase.
       -> L - Load extracted source data Amazon S3 datalakes in parquet format.  No transformations are 
       applied. Idea is to copy files from sources to S3 datalakes as and when they are available so that downstream applications can 
       source data from S3 datalakes only.
       -> T - Profile data from S3 datalake and load back the necessary subset of data into S3. Data processing is done using Apache Spark
       -> T - Transform profiled data from S3 , aggregate as needed and load into staging area of Redshift database. From there transfer data into star schema modeled tables such as Dimensions and Facts in Amazon Redshift for data analysis using Apache Spark
     
   ### STAR Schema
       Fact - Immigration
       Dimensions: Date, Visa, Entry_Port, Entry_Mode, Citizenship_Country and Destination_State
       
   ### Technology Stack
       AWS S3: 
           -> For datalakes to store extracted data from source systems including other sources to enrich data for current and future usage 
           
       Pandas: 
           -> Python pandas are used for extraction and loading into S3 datalakes
       
       Apache Spark:
           -> Spark (Pyspark) is used to profile the data, and trasnform and transfer into Redshift databases for efficient and highly scalable work loads
       
       AWS Redshift: 
           -> Used as target system to store STAR schema modeled Facts and Dimension tables for data analysis               
                      
## Data Analysis
    Sample data analysis questions need to be answered by target system are:
        1. Top 5 visatypes immigrated in year 2016?
            SELECT vd.visatype,
                sum(f.count) as totalcount
            FROM 
                immigration.immigration f
            JOIN
                immigration.visa vd
            ON 
                f.visatype=vd.visatype
            JOIN
                immigration.date dd
            ON
                f.arrdate=dd.date
                AND
                    dd.year=2016
            GROUP BY
                vd.visatype
            ORDER BY
                totalcount desc
            LIMIT 5
        
        2. Top 5 visa categories immigrated by Femaile gender in year 2016?
            SELECT vd.visa_category,
                f.gender,
                sum(f.count) as totalcount
            FROM 
                immigration.immigration f
            JOIN
                immigration.visa vd
            ON 
                f.visatype=vd.visatype
            JOIN
                immigration.date dd
            ON
                f.arrdate=dd.date
                AND
                    dd.year=2016
            WHERE 
                f.gender='F'
            GROUP BY
                vd.visa_category,
                f.gender
            ORDER BY
                totalcount desc
            LIMIT 5
        
        3. Name a state that most number of immigrants moved for each visatype from year 2016?
            SELECT vd.visatype,
                dsd.destination_state,
                sum(f.count) totalcount
            FROM 
                immigration.immigration f
            JOIN
                immigration.visa vd
            ON 
                f.visatype=vd.visatype
            JOIN
                immigration.date dd
            ON
                f.arrdate=dd.date
                AND
                    dd.year=2016
            JOIN
                immigration.destination_state ds
            ON
                ds.id=f.i94addr
            GROUP BY
                vd.visatype,
                dsd.destination_state
            ORDER BY
                totalcount desc
        
        4. Name top 5 countries that most number of female immigrants migrated from other than India and China since 2016?
            SELECT ccd.country_of_citizenship,
                sum(f.count) as totalcount
            FROM 
                immigration.immigration f
            JOIN
                immigration.citizenship_country ccd
            ON 
                f.i94cit=ccd.id
            JOIN
                immigration.date dd
            ON
                f.arrdate=dd.date
                AND
                dd.year=2016
            WHERE
                f.gender='F'
                AND
                ccd.country_of_citizenship not in ("India", "China")
            GROUP BY
                ccd.country_of_citizenship
            ORDER BY
                totalcount desc
            LIMIT 5
        
        5. Top mode of entry other than Air in year 2016?
            SELECT emd.mode_of_entry,
                sum(f.count) as totalcount
            FROM 
                immigration.immigration f
            JOIN
                immigration.entry_mode emd
            ON 
                f.i94mode=emd.id
            JOIN
                immigration.date dd
            ON
                f.arrdate=dd.date
                AND
                dd.year=2016
            WHERE
                emd.mode_of_entry != 'Air'
            GROUP BY
                emd.mode_of_entry
            ORDER BY
                totalcount desc
            LIMIT 1
            
        6. Top 5 port of entries in US by immigrants in year 2016?
            SELECT epd.port_of_entry,
                sum(f.count) as totalcount
            FROM 
                immigration.immigration f
            JOIN
                immigration.entry_mode emd
            ON 
                f.i94port=epd.id
            JOIN
                immigration.date dd
            ON
                f.arrdate=dd.date
                AND
                dd.year=2016
            GROUP BY
                epd.port_of_entry
            ORDER BY
                totalcount desc
            LIMIT 5
        
## Scripts
    aws.cfg:
        > Use this configuration file to provide S3 buckets, AWS Role and AWS Redshift cluster details of your choice.

    extract_to_datalakes.py: 
        > Run this script to extract files from their respective sources and load into S3 datalakes
        > Also to check if all source files are extracted successfully to target for each source type
        
    dataprofile.py: 
        > Run this script to perform data cleaning and data profiling activities on all the files in datalakes and load into S3 datalakes under profiled bucket
        > Also to check if all files are profiled successfully of each source type
        
    transform_to_datawarehouse.py:
        > Run this script to transform immigration and enrichment data and load into Redshift staging area. 
        > Populate all the star schema modeled fact and dimension tables from data in staging area
        > Also to perform data quality checks in each stage
        
## Other scenarios
    What if data was increased by 100 times?
        > Configure Spark to enable parallelism and create more work nodes to distribute the data and process parallelly in all work nodes at the same 
   
    What if you want to run the pipelines daily at 7AM?
        > The entire stages of the ELTT can be tranformed into data pipelies using pipelines archestration tools like Apache Airflow
        
    What if database is needed by more than 100 people?
        > Add more nodes to the existing Redshift cluster if performance of the queries is slow when more users are accessing simultaneously.
        
    
        
        
        
           