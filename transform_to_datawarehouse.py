# Download RedshiftJDBC42-no-awssdk-1.2.34.1058.jar file and refer in spark session
import configparser
#pip install s3fs
import s3fs
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

config = configparser.ConfigParser()
config.read('aws.cfg')

aws_key_id=config['AWS']['AWS_ACCESS_KEY_ID']
aws_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_ACCESS_KEY_ID']=aws_key_id
os.environ['AWS_SECRET_ACCESS_KEY']=aws_access_key

username = config['CLUSTER']['REDSHIFT_USER_NAME']
password = config['CLUSTER']['REDSHIFT_PASSWORD']
redshiftHostname = config['CLUSTER']['REDSHIFT_HOST_NAME']
redshiftPort = config['CLUSTER']['REDSHIFT_PORT']
credentials=config['IAM_ROLE']['ARN']

redshiftUrl = "jdbc:redshift://{}:{}/dev?user={}&password={}".format(redshiftHostname,redshiftPort,username,password)
credentials="arn:aws:iam::810626018418:role/myRedshiftRole"

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
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3,com.databricks:spark-redshift_2.10:3.0.0-preview1,com.amazonaws:aws-java-sdk:1.7.4,com.databricks:spark-avro_2.10:2.0.1") \
        .config("spark.jars", "RedshiftJDBC42-no-awssdk-1.2.34.1058.jar") \
        .config("spark.hadoop.fs.s3a.access.key", aws_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_access_key) \
        .getOrCreate()
    return spark


def transform_immigration_data(spark, source, enrich):
    """
    Description: 
        This function is to transform profiled immigration and enrichment data and load into Redshift staging area using spark.  
    Arguments: 
        spark session
        source: location of profiled sources
        enrich: location of profiled enrichment sources
    Returns: 
        None
    """
    print("INFO: Transform immigration data into data warehouse")    
               
    #Read enrich I94VISA data
    enrich=enrich.replace("s3", "s3a")
    df_enrich_visa=spark.read.parquet(enrich+"I94Visa.parquet")
    
    #Read enrich I94CIT_I94RES data
    df_enrich_res=spark.read.parquet(enrich+"I94CIT_I94RES.parquet")
    
    #Read enrich I94ADDR data 
    df_enrich_addr=spark.read.parquet(enrich+"i94addr.parquet")
    
    #Read enrich I94Mode data
    df_enrich_mode=spark.read.parquet(enrich+"I94Mode.parquet")
    
    #Read enrich I94Port data
    df_enrich_port=spark.read.parquet(enrich+"I94Port.parquet")
    
    fs=s3fs.S3FileSystem(anon=False)    
    immigration_filenames=[]
    for immigration_filename in fs.ls(source):
        if immigration_filename.endswith('.parquet'):
            immigration_filename='s3://'+immigration_filename
            immigration_filenames.append(immigration_filename)
    
    for source_parquet_file in immigration_filenames:
        source_parquet_file_name=Path(source_parquet_file).name
        if os.path.exists("test"):
            print("INFO: {} already profiled".format(source_parquet_file_name))
        else:
            source_parquet_file=source_parquet_file.replace("s3","s3a")
            df = spark.read.parquet(source_parquet_file)
            
            immigration_df=df.selectExpr("arrdate","i94cit","i94port","i94mode","i94addr","gender","visatype","i94visa","count")
            immigration_summary_df=immigration_df.groupBy("arrdate","i94cit","i94port","i94mode","i94addr","gender","visatype","i94visa").agg(sum("count").alias("count"))
            
            #join enrich visa data
            immigration_stage_table=immigration_summary_df.join(df_enrich_visa, df_enrich_visa.code==immigration_summary_df.i94visa, 'left_outer')
            immigration_stage_table=immigration_stage_table.selectExpr("arrdate","i94cit","i94port","i94mode","i94addr","gender","visatype","i94visa", "value as visa_category","count")
         
            #Join enrich country of citizenship data
            immigration_stage_table=immigration_stage_table.join(df_enrich_res, df_enrich_res.code==immigration_stage_table.i94cit, 'left_outer')
            immigration_stage_table=immigration_stage_table.selectExpr("arrdate","i94cit","value as country_of_citizenship","i94port","i94mode","i94addr","gender","visatype","i94visa","visa_category","count")
           
            #join enrich destination state data
            immigration_stage_table=immigration_stage_table.join(df_enrich_addr, df_enrich_addr.code==immigration_stage_table.i94addr, 'left_outer')
            immigration_stage_table=immigration_stage_table.selectExpr("arrdate","i94cit","country_of_citizenship","i94port","i94mode", "i94addr","value as destination_state","gender","visatype","i94visa","visa_category","count")
            
            #Join enrich port of entry data
            immigration_stage_table=immigration_stage_table.join(df_enrich_port, df_enrich_port.code==immigration_stage_table.i94port, 'left_outer')
            immigration_stage_table=immigration_stage_table.selectExpr("arrdate","i94cit","country_of_citizenship","i94port","value as port_of_entry", "i94mode","i94addr","destination_state","gender","visatype","i94visa","visa_category","count")
            
            #Join enrich mode of entry data
            immigration_stage_table=immigration_stage_table.join(df_enrich_mode, df_enrich_mode.code==immigration_stage_table.i94mode)
            immigration_stage_table=immigration_stage_table.selectExpr("arrdate","i94cit","country_of_citizenship","i94port","port_of_entry", "i94mode","value as mode_of_entry","i94addr","destination_state","gender","visatype","i94visa","visa_category","count")
            
            #get all columns with null values
            null_columns=getColumnsWithNulls(immigration_stage_table)
            immigration_stage_table=immigration_stage_table.fillna('missing', subset=null_columns)
            
            print("INFO: copy data into redshift table")
            populate_table(immigration_stage_table,"immigration_stage", "immigration_stage")

            
def create_star_schema(spark):
    """
    Description: 
        This function is to transform data from Redshift staging area into public star schema using spark.  
    Arguments: 
        spark session
    Returns: 
        None
    """
    stage_df=read_from_table(spark,"immigration_stage","immigration_stage")
    
    public_schema="immigration"
    #populate visa dimension table
    print("INFO: Populating visa dimention table")
    visa_df=stage_df.selectExpr("visatype","i94visa as visa_category_code","visa_category").distinct()
    populate_table(visa_df, public_schema, "visa")
    
    #populate citizenship_country dimension table
    print("INFO: Populating citizenship_country dimention table")
    cit_df=stage_df.selectExpr("i94cit as id","country_of_citizenship").distinct()
    populate_table(cit_df, public_schema, "citizenship_country")
    
    #populate entry_port dimension table
    print("INFO: Populating entry_port dimention table")
    port_df=stage_df.selectExpr("i94port as id","port_of_entry").distinct()
    populate_table(port_df, public_schema, "entry_port")
    
    #populate entry_mode dimension table
    print("INFO: Populating entry_mode dimention table")
    mode_df=stage_df.selectExpr("i94mode as id","mode_of_entry").distinct()
    populate_table(mode_df, public_schema, "entry_mode")
    
    #populate destination_state dimension table
    print("INFO: Populating destination_state dimention table")
    addr_df=stage_df.selectExpr("i94addr as id","destination_state").distinct()
    populate_table(addr_df, public_schema, "destination_state")
    
    #populate immigration fact table
    print("INFO: Populating immigration fact table")
    immigration_df=stage_df.selectExpr("arrdate","i94cit","i94port","i94mode","i94addr","gender","visatype","count")
    immigration_summarr_df=immigration_df.groupBy("arrdate","i94cit","i94port","i94mode","i94addr", "gender","visatype").agg(sum("count").alias("count"))
    populate_table(immigration_summarr_df, public_schema, "immigration")
    
    print("INFO: Populating date dimention table")
    table_df=read_from_table(public_schema,"immigration")
    date_df=table_df.selectExpr("arrdate").distinct()
    date_df=date_df.select(date_df[arrdate].alias('date'), day(date_df[arrdate]).alias('day'),week(date_df[arrdate]).alias('week'), month(date_df[arrdate]).alias('month'),year(date_df[arrdate]).alias('year'))
    populate_table(date_df, public_schema, "date")
    
    
def populate_table(table_df, schema_name, table_name):
    """
    Description: 
        This function is to populate data from dataframe into given table.  
    Arguments: 
        table_df dataframe
        schema_name name of schema
        table_name name of table
    Returns: 
        None
    """
    table_df.write.format("jdbc") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .option("url", redshiftUrl) \
        .option("dbtable", "{}.{}".format(schema_name, table_name)) \
        .option("user", username) \
        .option("password", password) \
        .option("aws_iam_role", credentials) \
        .mode("append") \
        .save()
    
def read_from_table(spark, schema_name, table_name):
    """
    Description: 
        This function is to read data from given Redshift table.  
    Arguments: 
        spark sparksession
        schema_name name of schema
        table_name name of table
    Returns: 
        table_df dataframe
    """
    table_df=spark.read.format("jdbc") \
              .option("driver", "com.amazon.redshift.jdbc42.Driver") \
              .option("url", redshiftUrl) \
              .option("dbtable", "{}.{}".format(schema_name, table_name)) \
              .option("user", username) \
              .option("password", password) \
              .option("aws_iam_role", credentials) \
              .load()
    return table_df
    
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
            

def check_staging_data_quality(spark):
    """
    Description: 
        This is to perform data quality checks for staging area
    Arguments: 
        spark sparksession
    Returns: 
        None
    """
    table_name="immigration_stage"
    schema_name="immigration_stage"
    df=read_from_table(spark, schema_name, table_name)
    if is_table_empty(df)==0:
        print("INFO: Table {}.{} is not empty".format(schema_name, table_name))
    else:
        print("ERROR: Table {}.{} is empty. validate the transform_to_datawarehouse again".format(schema_name, table_name))
    
def check_public_data_quality(spark):
    """
    Description: 
        This is to perform data quality checks for staging area
    Arguments: 
        spark sparksession
    Returns: 
        None
    """
    schema_name="immigration"
    tables_list=["immigration","visa","entry_port","entry_mode","citizenship_country","destination_state","date"]
    for table_name in tables_list:
        df=read_from_table(spark, schema_name, table_name)
        if is_table_empty(df)==0:
            print("INFO: Table {}.{} is not empty".format(schema_name, table_name))
        else:
            print("ERROR: Table {}.{} is empty. validate the transform_to_datawarehouse again".format(schema_name, table_name))
        

def is_table_empty(dataframe):
    """
    Description: 
        This is to check if table is empty
    Arguments: 
        dataframe 
    Returns: 
        Boolean
    """
    if dataframe.count() > 0:
        return 1
    else:
        return 0


def main():
    
    spark = create_spark_session()
     
    # Transform immigration and enrichment data and load into Redshift staging area
    immigration_local_profiled_datalake="datalakes/immigration/profiled/"
    immigration_s3_profiled_datalake="{}/immigration/profiled/".format(config['S3']['DATALAKE']).replace("'", "")
    immigration_s3_enrich_profiled_datalake="{}/immigration/enrich/profiled/".format(config['S3']['DATALAKE']).replace("'", "")   
    #transform_immigration_data(spark, immigration_s3_profiled_datalake, immigration_s3_enrich_profiled_datalake)  
    
    #Perform Data quality checks for staging area
    check_staging_data_quality(spark)
    
    #Transform staging area into public areas
    #create_star_schema(spark) 
    
    #Perform Data quality checks for star schema
    check_public_data_quality(spark)
 

if __name__ == "__main__":
    main()
