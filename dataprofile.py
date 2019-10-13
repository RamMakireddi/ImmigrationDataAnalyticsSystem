import configparser
import os
import glob
import pandas as pd
import datetime
#pip install s3fs
import s3fs
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf,when

config = configparser.ConfigParser()
config.read('aws.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Description: 
        This function is to create spark session
    Arguments: 
        None
    Returns: 
        spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def profile_immigration_data(spark, source, target):
    """
    Description: 
        This function is to perform Exploratory Data Analysis and data profiling on immigration data and load back into s3 data lakes in parquet format using spark.  
    Arguments: 
        spark session
        source: location for source parquet files
        target: location for target parquet files
    Returns: 
        None
    """
    print("INFO: Profiling immigration data")    
    fs=s3fs.S3FileSystem(anon=False)
    filenames=[]
    for filename in fs.ls(source):
        if filename.endswith('.parquet'):
            filename='s3://'+filename
            filenames.append(filename)
    
    for source_parquet_file in filenames:
        source_parquet_file_name=Path(source_parquet_file).name
        profiled_parquet_file_path=target+source_parquet_file_name
        if fs.exists(profiled_parquet_file_path):
            print("INFO: {} already profiled".format(source_parquet_file_name))
        else:
            source_parquet_file=source_parquet_file.replace("s3", "s3a")
            profiled_parquet_file_path=profiled_parquet_file_path.replace("s3", "s3a")
            df = spark.read.parquet(source_parquet_file)            
           
            #convert all necessary double type columns to long
            columns_to_long=['cicid', 'admnum']
            df=convert_data_types(df,columns_to_long, LongType())
            
            #convert all necessary double type columns to int
            columns_to_int=['i94yr','i94mon','i94cit','i94res','i94mode','i94bir','i94visa','count','biryear']
            df=convert_data_types(df,columns_to_int, IntegerType())
            
            #convert all necessary binary columns to string type
            columns_to_string=['i94port','i94addr','visapost','occup','entdepa','entdepd','matflag','gender','airline','visatype','fltno','insnum','dtadfile', 'dtaddto','entdepu']
            df=convert_data_types(df,columns_to_string, StringType())
            
            #convert sas int type date columns to dateformat
            sas_columns_to_date=['arrdate', 'depdate']
            df=convert_sas_date(df,sas_columns_to_date)               
            
            #get all columns with null values
            null_columns=getColumnsWithNulls(df)
            print(null_columns)
            
            total_rows=df.count()
            
            #Identify columns if total null values are more than 30% of total rows
            columns_to_drop=[]
            for null_column in null_columns:
                if hasMoreNulls(total_rows, df.where(df[null_column].isNull()).count())==1:
                    columns_to_drop.append(null_column)
                    print("INFO: {} - {}".format(null_column, df.where(df[null_column].isNull()).count()))
                         
            #drop columns that have 30% or more null values assuming this is true across all across all the immigration files
            df = df.drop(*columns_to_drop)
            
            #list of null columns still remain in data frame with null values 
            print("INFO: columns_with_nulls - {}".format(null_columns))
            print("INFO: columns_to_drop - {}".format(columns_to_drop))
            columns_not_dropped=[x for x in null_columns if x not in columns_to_drop ]        
            print("INFO: Columns not dropped - {}".format(columns_not_dropped))
            
            #set default values for missing string values
            df=df.fillna('missing', subset=columns_not_dropped)
            
            #Load profiled airports data back to datalakes
            df.write.parquet(profiled_parquet_file_path)
            
def validate_dataprofile(sourcetype, source, target):
    """
    Description: 
        This function is to check if all the files are dataprofiled successfully.  
    Arguments: 
        source: location of source
        target: location of target
    Returns: 
        None
    """
    print("INFO: validate data profiling for {} ".format(sourcetype))
    source_files=glob.glob(""+source+"/*")
    fs=s3fs.S3FileSystem(anon=False)
    no_of_source_files= get_no_of_files(source)
    no_of_target_files=get_no_of_files(target)  
    if no_of_source_files==no_of_target_files:
        print("INFO: Successfully data profiled all the {} files in source to target".format(sourcetype))
    else:
        print("ERROR: {} files out {} files only data profiled successfully".format(no_of_target_files, no_of_source_files))


def get_no_of_files(bucket):
    """
    Description: 
        This function is to return number of files in S3 bucket  
    Arguments: 
        bucket: location of s3 bucket
    Returns: 
        number of files
    """
    fs=s3fs.S3FileSystem(anon=False)
    filenames=[]
    for filename in fs.ls(bucket):
        if filename.endswith('.*'):
            filenames.append(filename)
    return len(filenames)
            

def profile_immigration_enrichment_data(spark, source, target):
    """
    Description: 
        This function is to extract immigration labels description files in text and load into s3 data lakes in csv format.  
    Arguments: 
        spark: spark session
        source: location for source enrichment files
        target: location for target csv files
    Returns: 
        None
    """
    print("INFO: Profiling immigration enrichmment data")
    fs=s3fs.S3FileSystem(anon=False)
    filenames=[]
    for filename in fs.ls(source):
        if filename.endswith('.csv'):
            filename='s3://'+filename
            filenames.append(filename)
        
    for csv_file in filenames:
        csv_file_name=Path(csv_file).name
        parquet_file_name=csv_file_name.split('.')[0]+".parquet"
        profile_enrich_file_path=target+parquet_file_name               
        if fs.exists(profile_enrich_file_path):
            print("INFO: {} already processed".format(csv_file_name))
        else:
            print("INFO: {} is in process".format(csv_file_name))     
            spaceDeleteUDF = udf(lambda s: s.replace(" ", ""), StringType())
            subStrAddrUDF = udf(lambda s: s[-2:], StringType())
            subStrPortUDF = udf(lambda s: s[0:-1], StringType())
            
            csv_file=csv_file.replace("s3", "s3a")
            profile_enrich_file_path=profile_enrich_file_path.replace("s3","s3a")
            
            df_enrich=spark.read.csv(csv_file,header=True, inferSchema=True)
            if csv_file_name not in ["I94Port.csv", "i94addr.csv"]:
                df_enrich=convert_data_types(df_enrich,['code'],IntegerType())
            else: 
                df_enrich=df_enrich.withColumn("code", spaceDeleteUDF("code")) 
                if csv_file_name=="i94addr.csv":
                    df_enrich=df_enrich.withColumn("code", subStrAddrUDF("code"))
                else:
                    df_enrich=df_enrich.withColumn("code", subStrPortUDF("code"))
            df_enrich.write.parquet(profile_enrich_file_path)
            
                        
def convert_sas_date(dataframe, columns):
    """
    Description: 
        This function is to convert sas date format to calender date format  
    Arguments: 
        dataframe: dataframe
        columns: list of columns to be converted
    Returns: 
        dataframe
    """
    epoch=datetime.datetime(1960, 1, 1)
    convert_to_date=udf(lambda s: epoch+datetime.timedelta(days=int(s)) if s is not None else None, DateType())
    for column in columns:        
        dataframe=dataframe.withColumn(column, convert_to_date(column))
    return dataframe


def convert_data_types(dataframe, columns, datatype):
    """
    Description: 
        This function is to convert list of columns to given datatype  
    Arguments: 
        dataframe: dataframe
        columns: list of columns to be converted
        datatype: datatype to be converted to
    Returns: 
        dataframe
    """
    for column in columns_list:
        dataframe=dataframe.withColumn(column, dataframe[column].cast(datatype))
    return dataframe
    
                
def profile_airports_data(spark, source, target):
    """
    Description: 
        This function is to perform Exploratory Data Analysis and data profiling on airports data and load back into s3 data lakes in parquet format using spark.  
    Arguments: 
        spark session
        source: location for source parquet file
        target: location for target parquet file
    Returns: 
        None
    """
    print("INFO: Profiling airports data")
    file_name=Path(source).name
    target_bucket=target
    target=target+file_name
    fs = s3fs.S3FileSystem(anon=False)
    if fs.exists(target):        
        print("INFO: {} already profiled".format(file_name))
    else:
        source=source.replace("s3", "s3a")
        target=target.replace("s3", "s3a")
        
        df = spark.read.load(source)
        
        #convert elevation_ft datatype to int
        columns_to_int=['elevation_ft']
        df =convert_data_types(df,columns_to_int, IntegerType())
       
        # Analyze why values are missing e.g., continent
        df.select("continent").distinct().show()
        #for Continent and country - NA i.e, North America has been stored as Null so replace with NA string
        columns_to_clean=['continent', 'iso_country']
        df = df.fillna('NA', subset=columns_to_clean)
        
        #For local_code fill ident if no value
        df = df.withColumn('airport_code', when(df['local_code'].isNull(), df['iata_code']).otherwise(df['local_code']))
        df = df.withColumn('airport_code', when(df['airport_code'].isNull(), df['ident']).otherwise(df['airport_code']))
        df = df.withColumn('airport_code', when(df['iata_code'].isNull(), df['airport_code']).otherwise(df['iata_code']))
        
        #set default as values for missing elevation feet
        df=df.fillna(-1000, subset=['elevation_ft'])
        
        #Load profiled airports data back to datalakes
        df.write.parquet(target)       
        
 
def getColumnsWithNulls(df):
    """
    Description: 
        This function is to get list of columns with null values  
    Arguments: 
        dataframe: dataframe
    Returns: 
        list of null values
    """
    null_columns=[]
    for column in df.columns:
        if df.where(df[column].isNull()).count() > 0:
             null_columns.append(column)
    return null_columns

def hasMoreNulls(total, actual):
    """
    Description: 
        This function is to check if number of nulls of that column has more than 30% of total rows
    Arguments: 
        total: total rows
        actual: number of rows with null values
    Returns: 
        Boolean 
    """
    if actual/total>.3:
        return 1
    else:
        return 0


def main():
     
    spark = create_spark_session()
    
    # profile airports data and load back into datalakes
    airport_local_datalake="datalakes/airports/airport-codes.parquet"
    airport_s3_datalake="{}/airports/airport-codes.parquet".format(config['S3']['DATALAKE']).replace("'", "") 
    airport_local_profiled_datalake="datalakes/airports/profiled/airport-codes.parquet"
    airport_s3_profiled_datalake="{}/airports/profiled/".format(config['S3']['DATALAKE']).replace("'", "") 
    profile_airports_data(spark, airport_s3_datalake, airport_s3_profiled_datalake) 
    validate_dataprofile("Airports", airport_s3_datalake, airport_s3_profiled_datalake)
       
    # profile immigration data and load back into datalakes
    immigration_S3_datalake="{}/immigration/".format(config['S3']['DATALAKE']).replace("'", "")
    immigration_local_datalake="datalakes/immigration/"
    immigration_local_profiled_datalake="datalakes/immigration/profiled/"
    immigration_S3_profiled_datalake="{}/immigration/profiled/".format(config['S3']['DATALAKE']).replace("'", "")
    profile_immigration_data(spark, immigration_S3_datalake, immigration_S3_profiled_datalake)
    validate_dataprofile("Immigration", immigration_S3_datalake, immigration_S3_profiled_datalake)
    
    # profile immigration enrichment data and load back into datalakes
    immigration_enrich_s3_datalake="s3://ram-udacity-dend/capstone/datalakes/immigration/enrich/"
    immigration_enrich_s3_profiled_datalake="s3://ram-udacity-dend/capstone/datalakes/immigration/enrich/profiled/"
    profile_immigration_enrichment_data(spark, immigration_enrich_s3_datalake, immigration_enrich_s3_profiled_datalake)
    validate_dataprofile("Enrichment", immigration_enrich_s3_datalake, immigration_enrich_s3_profiled_datalake)

if __name__ == "__main__":
    main()
