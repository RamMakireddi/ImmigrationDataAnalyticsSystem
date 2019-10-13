#pip install pyarrow
#pip install pandas==0.24.2 --force-reinstall
import configparser
#pip install s3fs
import s3fs
import os
import glob
import pandas as pd
#pip install datapackage
import datapackage 
from pathlib import Path


config = configparser.ConfigParser()
config.read('aws.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def extract_immigration_data(source, target):
    """
    Description: 
        This function is to extract immigration data in sas7bdat format files and load into s3 data lakes in parquet format using pandas.  
    Arguments: 
        source: location for source sas7bdat files
        target: location for target parquet files
    Returns: 
        None
    """
    print("INFO:Extracting and loading immigration data into datalakes")
    filenames = glob.glob(""+source+"/*")
    fs = s3fs.S3FileSystem(anon=False)
    
    for sasfile in filenames:
        sas_file_name=Path(sasfile).name
        parquet_file_name=sas_file_name.split('.')[0].split('_')[1]+".parquet"
        parquet_file_path=target+parquet_file_name
        if fs.exists(parquet_file_path):
            print("INFO: {} already processed".format(parquet_file_name))
        else:
            pdf = pd.read_sas(sasfile)
            pdf.to_parquet(parquet_file_path)
            

def extract_immigration_enrichment_data(source, target):
    """
    Description: 
        This function is to extract immigration labels description files in text and load into s3 data lakes in csv format.  
    Arguments: 
        source: location for source enrichment files
        target: location for target csv files
    Returns: 
        None
    """
    print("INFO: Extracting and loading immigration enrichmment data")
    filenames = glob.glob(""+source+"/*")
    fs = s3fs.S3FileSystem(anon=False)
    
    for textfile in filenames:
        text_file_name=Path(textfile).name
        csv_file_name=text_file_name.split(".")[0]+".csv"
        csv_file_path=target+csv_file_name        
        
        if fs.exists(csv_file_path):
            print("INFO: {} already processed".format(csv_file_name))
        else:
            print("INFO: {} is in process".format(text_file_name))
            data = open(textfile).read()
            lines_of_data = data.splitlines()
            tmp = []
            
            tmp.append(["code", "value"])
            for line in lines_of_data:
                tmp.append(line.replace("'", "").split("="))
            df = pd.DataFrame(tmp) 
            df.to_csv(csv_file_path, index = None, header=False)
                        
        
def extract_airports_data(source, target):
    """
    Description: 
        This function is to extract airports codes data file in csv format and load into s3 data lakes in parquet format using pandas.  
    Arguments: 
        source: location for source json file
        target: location for output parquet file
    Returns: 
        None
    """
    print("INFO: Extracting and loading airports data")
    # to load Data Package into storage
    package = datapackage.Package(source)
    # to load only tabular data
    resources = package.resources
    for resource in resources:
        if resource.descriptor['datahub']['type']=='derived/csv':
            parquet_file_name=resource.name.split('_')[0]+".parquet"
            parquet_file_path=os.path.join(target, parquet_file_name)          
            fs = s3fs.S3FileSystem(anon=False)
            if fs.exists(parquet_file_path):        
                print("INFO: {} already processed".format(parquet_file_name))
            else:
                df = pd.read_csv(resource.descriptor['path'])
                df.to_parquet(parquet_file_path) 


def validate_extract_to_datalakes(sourcetype, source, target):
    """
    Description: 
        This function is to check if all the files are source are extracted to target airports codes data file in csv format and load into s3 data lakes in parquet format using pandas.  
    Arguments: 
        source: location of source
        target: location of target
    Returns: 
        None
    """
    print("INFO: validate {} extract".format(sourcetype))
    source_files=glob.glob(""+source+"/*")
    no_of_source_files= len(glob.glob(""+source+"/*"))
    fs=s3fs.S3FileSystem(anon=False)
    target_filenames=[]
    for filename in fs.ls(target):
        if filename.endswith('.*'):
            print(filename)
            target_filenames.append(filename)
    no_of_target_files=len(target_filenames)
    
    if no_of_source_files==no_of_target_files:
        print("INFO: Successfully extracted all the {} files in source to target".format(sourcetype))
    else:
        print("ERROR: {} files out {} files only extracted successfully".format(no_of_target_files, no_of_source_files))
    
        
def main():    
    source_enrichment_data = config['SOURCE']['SOURCE_ENRICHMENT'].replace("'", "")
    immigration_enrich_local_datalake="datalakes/immigration/enrich/"
    immigration_enrich_S3_datalake='{}/immigration/enrich/'.format(config['S3']['DATALAKE']).replace("'", "")   
    #Extract enrichment data into datalakes
    extract_immigration_enrichment_data(source_enrichment_data, immigration_enrich_S3_datalake)    
    #Check if all the enrich files are loaded successfully into S3
    validate_extract_to_datalakes("Enrichment", source_enrichment_data, immigration_enrich_S3_datalake)
        
    #source_airport_data="airport-codes_csv.csv"
    source_airport_data=config['SOURCE']['SOURCE_AIRPORT'].replace("'", " ")
    airport_local_datalake="datalakes/airports/"
    airport_s3_datalake="{}/airports/".format(config['S3']['DATALAKE']).replace("'", "")   
    # Extract and load airport codes data into datalakess
    extract_airports_data(source_airport_data, airport_s3_datalake)    
    #Check if all the airports files are loaded successfully into S3
    validate_extract_to_datalakes("Airports", source_airport_data, airport_s3_datalake)
    
    source_immigration_data = config['SOURCE']['SOURCE_IMMIGRATION']
    immigration_local_datalake="datalakes/immigration/"
    immigration_S3_datalake="{}/immigration/".format(config['S3']['DATALAKE']).replace("'", "")
    # Extract and load immigration data into datalakes
    extract_immigration_data(source_immigration_data, immigration_S3_datalake)    
    #Check if all the immigration files are loaded successfully into S3
    validate_extract_to_datalakes("Immigration", source_immigration_data, immigration_S3_datalake)        
    
if __name__ == "__main__":
    main()
