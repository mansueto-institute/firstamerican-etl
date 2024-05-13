import argparse, os, zipfile, sys, logging
from pathlib import Path
import psutil
import polars as pl

def mem_profile() -> str:
    """
    Return memory usage, str  [Function written by Nico Marchio]
    """
    mem_use = str(round(100 - psutil.virtual_memory().percent,4))+'% of '+str(round(psutil.virtual_memory().total/1e+9,3))+' GB RAM'
    return mem_use

def is_lazydataframe_empty(ldf):
    """
    Checks if a polars lazy dataframe is empty given a lazy dataframe.
    Returns: boolean (True, False)
    """
    return ((ldf.describe().filter(pl.col("statistic") == "count")["PropertyID"])[0] == 0)

def convert_sales(filename, input_dir):
    '''
    Convert zipped txt sales (deed) file into parquet format.

    Inputs:
    - filename: str ("Deed36061.txt.zip")
    - input_dir: str, path to directory where the file exists (and where all other files will be saved to).
        The filename above should be included within a subdirectory called "raw/".
    
    Returns: Nothing. Saves two parquet files to a 'staging/' subdirectory within the input_dir.
    - all sales parquet file ("Deed36061.parquet")
    - ranked sales parquet file ("ranked_Deed36061.parquet"): this parquet file
        only has the most recent sale for each year/propertyID combination. This 
        file is the one used for the join to create the "merged.parquet" file.
    
    Both files contain the propertyID, year, and sale amount.
    '''
    # filepaths
    unzipped_dir = input_dir+ "/" +'unzipped'
    input_filepath = input_dir + "/raw/" + filename
    output_dir = input_dir + "/" + "staging"
    output_filepath = output_dir + "/" + filename.replace(".txt.zip", ".parquet")
    output_filepath_ranked = output_dir + "/ranked_" + filename.replace(".txt.zip", ".parquet")

    # skip conversion if the file already exists
    if os.path.exists(output_filepath_ranked):
        logging.info(f"{output_filepath_ranked} already exists. Skipping this file in the directory...")
        return

    if not os.path.exists(output_filepath_ranked):
        # decompress file
        logging.info("Unzipping file...")
        unzipped_filepath = unzipped_dir + "/" + filename.replace(".txt.zip", ".txt")
        # if the unzipped file doesn't already exist, unzip it
        if not os.path.exists(unzipped_filepath):
            with zipfile.ZipFile(input_filepath, 'r') as zip_ref:
                zip_ref.extractall(unzipped_dir)

    # #convert all sales txt file to parquet
    # try:
        logging.info(f"Converting {input_filepath} to parquet...")
        (pl.scan_csv(Path(unzipped_filepath), separator = '|', low_memory = True, try_parse_dates=True, infer_schema_length=1000, ignore_errors = True, truncate_ragged_lines = True)
            .select(['PropertyID', 'SaleAmt', 'RecordingDate', 'FIPS', 'FATimeStamp', 'FATransactionID', 'TransactionType', 'SaleDate'])
                .filter(pl.col('PropertyID').is_not_null())
                .with_columns([(pl.col('SaleAmt').cast(pl.Int64))])
                .filter((pl.col('SaleAmt') > 0) & (pl.col('SaleAmt').is_not_null()))
                .with_columns(pl.col('RecordingDate').cast(pl.Utf8).str.slice(offset=0,length = 4).alias("RecordingYearSlice"))
                .with_columns([
                    (pl.col('PropertyID').cast(pl.Int64)),
                    # (pl.col('PropertyID').alias("PropertyID_str")),
                    (pl.col('FIPS').cast(pl.Utf8).str.pad_start(5, "0")),
                    (pl.col('RecordingDate').cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                    (pl.col('SaleDate').cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                    (pl.col('FATimeStamp').cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                    (pl.col('FATransactionID').cast(pl.Utf8).str.slice(offset=0,length = 1).alias("FATransactionID_1")),
                    (pl.when(pl.col('TransactionType').cast(pl.Utf8).is_in(['1', '2', '3', '4', '5', '6'])).then(pl.col('TransactionType').cast(pl.Utf8)).otherwise(None).name.keep()),
                    ])
                .with_columns([
                    (pl.col("RecordingDate").dt.year().alias("RecordingYear").cast(pl.Int64)),
                    (pl.col('SaleDate').dt.year().alias("SaleYear")),
                    (pl.col('FATimeStamp').dt.year().alias("FATimeStampYear")),
                    (pl.when((pl.col("FATransactionID_1").is_in(['1', '6'])) & (pl.col('TransactionType').is_in(['2', '3']))).then(1).otherwise(0).alias("SaleFlag")),
                    # check to make sure that the propID structure has been retained when converting to an int (1 if had a leading 0 or decimal, 0 otherwise)
                    # (pl.when((pl.col("PropertyID_str").str.starts_with('0') | (pl.col("PropertyID_str") != pl.col('PropertyID').cast(pl.String)))).then(1).otherwise(0).alias("PropIDFlag"))
                ])                     
            ).collect(streaming=True,
            ).write_parquet(Path(output_filepath), compression="snappy", use_pyarrow=True)
        logging.info(f"{output_filepath} complete.")

        #delete unzipped file for memory conservation
        logging.info("Deleting unzipped txt file...")
        os.remove(unzipped_filepath)
    
    # except Exception as e:
    #     if os.path.exists(output_filepath):
    #         os.remove(output_filepath)
    #     logging.info(f"Error: {str(e)}")
    #     sys.exit()

    # ranked sales file (only gets the most recent sale from each year/ID combination)
    #try:
    
    if not os.path.exists(output_filepath_ranked):

        logging.info(f"Creating {output_filepath_ranked}...")
        sale_ranked = (pl.scan_parquet(Path(output_filepath), low_memory = True, parallel='row_groups', use_statistics=False, hive_partitioning=False)
            .filter(pl.col('SaleFlag') == 1)
            .with_columns([
                (pl.col("RecordingDate").rank(method="random", descending = True, seed = 1).over(['RecordingYear', "PropertyID"]).alias("RecentSaleByYear")),
                (pl.col("RecordingDate").rank(method="random", descending = True, seed = 1).over(["PropertyID"]).alias("MostRecentSale")),
                (pl.col('PropertyID').cast(pl.Int64)),
                (pl.col('RecordingYear').cast(pl.Int64)),
                (pl.col('SaleAmt').cast(pl.Int64)),
            ])
            .filter(pl.col('RecentSaleByYear') == 1)
            ).select(['PropertyID', 'SaleAmt', 'RecordingYear']
            ).collect(streaming=True)

        sale_ranked.write_parquet(Path(output_filepath_ranked), use_pyarrow=True, compression="snappy")
        sale_ranked.clear()
        logging.info(f"{output_filepath_ranked} complete.")

    
    
    # except Exception as e:
    #     if os.path.exists(output_filepath_ranked):
    #         os.remove(output_filepath_ranked)
    #     logging.info(f"Error: {str(e)}")
    #     sys.exit()

    logging.info("remove just Deed file")
    os.remove(output_filepath)
    logging.info("Complete. Moving to next file...")

def convert_prop(filename, input_dir):
    '''
    Convert zipped txt prop (annual) file into parquet format.

    Inputs:
    - filename: str ("Prop36061.txt.zip")
    - input_dir: str, path to directory where the file exists (and where all other files will be saved to).
        The filename above should be included within a subdirectory called "raw/".
    
    Returns: Nothing. Saves parquet file to a parquet subdirectory within the input_dir.
    - all properties parquet file ("Prop36061.parquet"): this file contains
        time invariant characteristics of each property, including geographic
        characteristics and property classification.
    '''
    # filepaths
    unzipped_dir = input_dir+ "/" +'unzipped'
    input_filepath = input_dir + "/raw/" + filename
    output_dir = input_dir + "/" + "staging"
    output_filepath = output_dir + "/" + filename.replace(".txt.zip", ".parquet")

    # check if parquet already exists, if it does, skip
    if os.path.exists(output_filepath):
        logging.info(f"{output_filepath} already exists. Skipping this file in the directory...")
        return
    
    try:
        # decompress file
        logging.info("Unzipping file...")
        unzipped_filepath = unzipped_dir + "/" + filename.replace(".txt.zip", ".txt")
        # if the unzipped file doesn't already exist, unzip it
        if not os.path.exists(unzipped_filepath):
            with zipfile.ZipFile(input_filepath, 'r') as zip_ref:
                zip_ref.extractall(unzipped_dir)

        # convert annual file to parquet
        logging.info(f"Converting {input_filepath} to parquet...")
        # see https://github.com/mansueto-institute/fa-etl/blob/main/fa-etl.py#L127-L155
        (pl.scan_csv(unzipped_filepath, separator = '|', low_memory = True, try_parse_dates=True, infer_schema_length=1000, ignore_errors = True, truncate_ragged_lines = True)
            .select(['PropertyID', 'PropertyClassID', "FATimeStamp", 'SitusLatitude', 'SitusLongitude', 'SitusFullStreetAddress', 'SitusCity', 'SitusState', 'SitusZIP5', 'FIPS', 'SitusCensusTract', 'SitusCensusBlock', 'SitusGeoStatusCode'])
                .filter(pl.col('PropertyClassID') == 'R')
                .filter(pl.col('PropertyID').is_not_null())
                .with_columns([
                    (pl.col('PropertyID').cast(pl.Int64)),
                    (pl.col("FATimeStamp").cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                    (pl.when((pl.col('SitusLatitude') == 0)).then(None).otherwise(pl.col('SitusLatitude')).alias('SitusLatitude')),
                    (pl.when((pl.col('SitusLongitude') == 0)).then(None).otherwise(pl.col('SitusLongitude')).alias('SitusLongitude')),
                    (pl.col('FIPS').cast(pl.Utf8).str.pad_start(5, "0")),
                    (pl.col('SitusCensusTract').cast(pl.Utf8).str.pad_start(6, "0")),
                    (pl.col('SitusCensusBlock').cast(pl.Utf8).str.pad_start(4, "0")),
                    (pl.col('SitusZIP5').cast(pl.Utf8).str.pad_start(5, "0")),
                    (pl.when(pl.col('SitusGeoStatusCode').cast(pl.Utf8).is_in(['5', '7', '9', 'A', 'B', 'X', 'R'])).then(pl.col('SitusGeoStatusCode')).otherwise(None).name.keep()),
                    (pl.concat_str([pl.col("FIPS"), pl.col('SitusCensusTract')], separator= "_").fill_null(pl.col('FIPS')).alias("FIPS_SitusCensusTract"))
                    ])
            ).collect(streaming=True,
            ).write_parquet(output_filepath, compression="snappy", use_pyarrow=True)
        logging.info(f"{output_filepath} complete.")

    except Exception as e:
        if os.path.exists(output_filepath):
            os.remove(output_filepath)
        logging.info(f"Error: {str(e)}")
        sys.exit()

    #delete unzipped file for memory conservation
    logging.info("Deleting unzipped txt file...")
    os.remove(unzipped_filepath)
    logging.info("Complete. Moving to next file...")

def convert_taxhist(filename, input_dir):
    '''
    Convert zipped txt tax history file into parquet format.

    Inputs:
    - filename: str ("TaxHist36061.txt.zip")
    - input_dir: str, path to directory where the file exists (and where all other files will be saved to).
        The filename above should be included within a subdirectory called "raw/".
    
    Returns: Nothing. Saves parquet file to a parquet subdirectory within the input_dir.
    - all tax hist parquet file ("TaxHist36061.parquet"): this contains the 
        propertyID, tax year, and tax amount.
    '''
    # filepaths
    unzipped_dir = input_dir+ "/" +'unzipped'
    input_filepath = input_dir + "/raw/" + filename
    output_dir = input_dir + "/" + "staging"
    output_filepath = output_dir + "/" + filename.replace(".txt.zip", ".parquet")

    # check if parquet already exists, if it does, skip
    if os.path.exists(output_filepath):
        logging.info(f"{output_filepath} already exists. Skipping this file in the directory...")
        return

    try:
        # decompress file
        logging.info("Unzipping file...")
        unzipped_filepath = unzipped_dir + "/" + filename.replace(".txt.zip", ".txt")
        # if the unzipped file doesn't already exist, unzip it
        if not os.path.exists(unzipped_filepath):
            with zipfile.ZipFile(input_filepath, 'r') as zip_ref:
                zip_ref.extractall(unzipped_dir)
        
        # convert taxhist file to parquet
        logging.info(f"Converting {input_filepath} to parquet...")
        # see https://github.com/mansueto-institute/fa-etl/blob/main/fa-etl.py#L127-L155
        (pl.scan_csv(unzipped_filepath, separator = '|', low_memory = True, try_parse_dates=True, infer_schema_length=1000, ignore_errors = True, truncate_ragged_lines = True)
            .select(['PropertyID', 'TaxYear', 'TaxAmt'])
            .with_columns([
                (pl.col('PropertyID').cast(pl.Int64)),
                (pl.col('TaxYear').cast(pl.Int64)),
                (pl.col('TaxAmt').cast(pl.Int64)),
                #assumption that tax amount is off by 100
                (pl.col("TaxAmt").cast(pl.Int64)/100).alias("TaxAmtAdjusted"),
            ])
        ).sink_parquet(Path(output_filepath), compression="snappy")
        logging.info(f"{output_filepath} complete.")
    
    except Exception as e:
        if os.path.exists(output_filepath):
            os.remove(output_filepath)
        logging.info(f"Error: {str(e)}")
        sys.exit()

    #delete unzipped file for memory conservation
    logging.info("Deleting unzipped txt file...")
    os.remove(unzipped_filepath)
    logging.info("Complete. Moving to next file...")

def convert_valhist(filename, input_dir):
    '''
    Convert zipped txt value history file into parquet format.

    Inputs:
    - filename: str ("ValHist36061.txt.zip")
    - input_dir: str, path to directory where the file exists (and where all other files will be saved to).
        The filename above should be included within a subdirectory called "raw/".
    
    Returns: Nothing. Saves two parquet files to a parquet subdirectory within the input_dir.
    - all value history parquet file ("Deed36061.parquet") a cleaned / parquet version
        of the input txt file
    - ranked value history parquet file ("ranked_Deed36061.parquet"): this parquet file
        standardizes year across the assessed/market/appraised values, so that each observation
        is a propertyID/year combination with relevant values for each of these values (null
        if the value doesn't exist). There is a standardized "Value" column which takes the best 
        option of the 3 values when many are present (cascade logic: Assessed, Market, then Appraised)
        and retains the value used in a column called "AssessmentUsed" which contains str values ("Assd",
        "Market", "Appr"). This file also collapses the data a bit more by de-duplicating 
        multiple combinations of Year/PropertyID combinations.
    '''
    # filepaths
    unzipped_dir = input_dir+ "/" +'unzipped'
    input_filepath = input_dir + "/raw/" + filename
    output_dir = input_dir + "/" + "staging"
    output_filepath = output_dir + "/" + filename.replace(".txt.zip", ".parquet")
    output_filepath_temp1 = output_dir + "/rankedtemp1_" + filename.replace(".txt.zip", ".parquet")
    output_filepath_temp2 = output_dir + "/rankedtemp2_" + filename.replace(".txt.zip", ".parquet")
    output_filepath_ranked = output_dir + "/ranked_" + filename.replace(".txt.zip", ".parquet")

    # check if parquet already exists, if it does, skip
    if os.path.exists(output_filepath) & os.path.exists(output_filepath_ranked):
        logging.info(f"{output_filepath} and {output_filepath_ranked} already exists. Skipping this file in the directory...")
        return

    if not os.path.exists(output_filepath):
        # decompress file
        logging.info("Unzipping file...")
        unzipped_filepath = unzipped_dir + "/" + filename.replace(".txt.zip", ".txt")
        # if the unzipped file doesn't already exist, unzip it
        if not os.path.exists(unzipped_filepath):
            with zipfile.ZipFile(input_filepath, 'r') as zip_ref:
                zip_ref.extractall(unzipped_dir)

        # convert valhist file to parquet
        logging.info(f"Converting {input_filepath} to parquet...")
        # see https://github.com/mansueto-institute/fa-etl/blob/main/fa-etl.py#L127-L155
        (pl.scan_csv(unzipped_filepath, separator = '|', low_memory = True, try_parse_dates=True, infer_schema_length=1000, ignore_errors = True, truncate_ragged_lines = True)
            .select(['PropertyID', 'AssdTotalValue', 'AssdYear', 'MarketTotalValue', 'MarketValueYear', 'ApprTotalValue', 'ApprYear', 'TaxableYear'])
            .with_columns([
                (pl.col('PropertyID').cast(pl.Int64)),
                (pl.col('AssdTotalValue').cast(pl.Int64)),
                (pl.col('AssdYear').cast(pl.Int64)),
                (pl.col('MarketTotalValue').cast(pl.Int64)),
                (pl.col('MarketValueYear').cast(pl.Int64)),
                (pl.col('ApprTotalValue').cast(pl.Int64)),
                (pl.col('ApprYear').cast(pl.Int64)),
                (pl.col('TaxableYear').cast(pl.Int64)),
            ])
            ).sink_parquet(Path(output_filepath), compression="snappy")
        logging.info(f"{output_filepath} complete.")

        #delete unzipped file for memory conservation
        logging.info("Deleting unzipped txt file...")
        os.remove(unzipped_filepath)
    else:
        logging.info(f"{output_filepath} already exists. Moving on...")


    if not os.path.exists(output_filepath_ranked):
        logging.info(f"Creating {output_filepath_temp1}...")

        #temp filepaths
        assd_filepath = output_dir+"/assd.parquet"
        # market_filepath = output_dir+"/market.parquet"
        # appr_filepath = output_dir+"/appr.parquet"
        # logging.info(f"filepaths: {assd_filepath}, {market_filepath} and {appr_filepath}...")

        if not os.path.exists(assd_filepath):
            logging.info(f"Creating assd parquet...")
            (pl.scan_parquet(Path(output_filepath), low_memory = True, use_statistics=True, hive_partitioning=True)
                .with_columns([pl.col('AssdYear').cast(pl.Int64).alias('Year')])
                .with_columns([pl.col('AssdTotalValue').cast(pl.Int64).alias('Value')])
                .filter(
                    ((pl.col('AssdTotalValue').is_not_null()) & (pl.col('AssdYear').is_not_null())))
                .select(['PropertyID', 'Value', 'Year'])
                ).collect(streaming=True
                ).write_parquet(Path(assd_filepath), compression="snappy")
        
        if not os.path.exists(market_filepath):
            logging.info(f"Creating market parquet...")
            (pl.scan_parquet(Path(output_filepath), low_memory = True, use_statistics=True, hive_partitioning=True)
                .with_columns([pl.col('MarketValueYear').cast(pl.Int64).alias('Year')])
                .filter(
                    ((pl.col('MarketTotalValue').is_not_null()) & (pl.col('MarketValueYear').is_not_null())))
                .select(['PropertyID', 'MarketTotalValue', 'Year'])
                ).collect(streaming=True
                ).write_parquet(Path(market_filepath), compression="snappy")
        
        if not os.path.exists(appr_filepath):
            logging.info(f"Creating appr parquet...")
            (pl.scan_parquet(Path(output_filepath), low_memory = True, use_statistics=True, hive_partitioning=True)
                .with_columns([pl.col('ApprYear').cast(pl.Int64).alias('Year')])
                .filter(
                    ((pl.col('ApprTotalValue').is_not_null() & (pl.col('ApprYear').is_not_null()))))
                .select(['PropertyID', 'ApprTotalValue', 'Year'])
                ).collect(streaming=True
                ).write_parquet(Path(appr_filepath), compression="snappy")

        # write checks - make sure there are no duplicates in the above (by propID/year)
        #     if so, raise error and don't proceed
        assd = pl.scan_parquet(Path(assd_filepath), low_memory = True)
        appr = pl.scan_parquet(Path(appr_filepath), low_memory = True)
        market = pl.scan_parquet(Path(market_filepath), low_memory = True)

        logging.info(f"Joining assessed values and market values on propid/year...")
        # join with market data
        assd_market = assd.join(
            other=market,
            how="left",
            on=['PropertyID', 'Year'],
        ).collect(streaming=True
        ).write_parquet(Path(output_filepath_temp1), compression="snappy")

        logging.info(f"val/market join on propid/year complete. Starting second join...")

        rankedtemp1_valhist = pl.scan_parquet(Path(output_filepath_temp1), low_memory = True)
        logging.info(f"is ranked_valhist empty? {is_lazydataframe_empty(rankedtemp1_valhist)}")
        
        # check if the length of the output of a ldf is 0 (aka dataframe is empty)
        logging.info(f"Check if appraisal dataframe is empty...")
        if not is_lazydataframe_empty(appr):
            logging.info(f"Appraisal dataframe is not empty! Joining with val/market...")
            (rankedtemp1_valhist
                # # join with appr data
                ).join(
                    other=appr,
                    how="left",
                    on=['PropertyID', 'Year'],
                ).collect(streaming=True
                ).write_parquet(Path(output_filepath_temp2), compression="snappy")
        else:    
            logging.info(f"Appraisal dataframe is empty! Adding a col of nulls for appraisal col...")
            (rankedtemp1_valhist
                # add col of nulls for ApprTotalValue because not present for any PropIDs
                ).with_columns([
                    pl.when(True).then(None).alias("ApprTotalValue")
                ]).collect(streaming=True
                ).write_parquet(Path(output_filepath_temp2), compression="snappy")

        logging.info(f"val/market/appr join on propid/year complete. Doing with_cols operations...")
        (pl.scan_parquet(Path(output_filepath_temp2), low_memory = True)
            .with_columns([
                #value conditional
                pl.when((pl.col("AssdTotalValue").is_not_null()) & (pl.col("AssdTotalValue") != 0))
                    .then(pl.col("AssdTotalValue"))
                    .when((pl.col("MarketTotalValue").is_not_null()) & (pl.col("MarketTotalValue") != 0))
                    .then(pl.col("MarketTotalValue"))
                    .when((pl.col("ApprTotalValue").is_not_null()) & (pl.col("ApprTotalValue") != 0))
                    .then(pl.col("ApprTotalValue"))
                    .otherwise(None)
                    .alias("Value").cast(pl.Int64),
                #flag for which value is used
                pl.when((pl.col("AssdTotalValue").is_not_null()) & (pl.col("AssdTotalValue") != 0))
                    .then(pl.lit('Assd'))
                    .when((pl.col("MarketTotalValue").is_not_null()) & (pl.col("MarketTotalValue") != 0))
                    .then(pl.lit('Market'))
                    .when((pl.col("ApprTotalValue").is_not_null()) & (pl.col("ApprTotalValue") != 0))
                    .then(pl.lit('Appr'))
                    .otherwise(None)
                    .alias("AssessmentUsed")
            ]
        ).filter(
            (pl.col('AssessmentUsed') == "Assd")
        ).select(
            ['PropertyID','Year', 'Value', 'MarketTotalValue', 'ApprTotalValue']
        )).sink_parquet(Path(output_filepath_ranked), compression="snappy")

        logging.info(f"{output_filepath_ranked} complete.")

        #delete unzipped file for memory conservation
        logging.info("Deleting unranked file...")
        os.remove(output_filepath)

    logging.info("Complete. Moving to next file...")


def join(input_dir, ranked_valhist_filename, prop_filename, ranked_deed_filename, taxhist_filename):
    '''
    Creates one merged parquet file with an observation as each unique and 
    most recent combination of property id/year present in the value history file, 
    left joining in:
    - prop/annual file (property characterstics: geography, property class)
    - sales (sale amount)
    - tax history (tax amount by year)

    Inputs:
    - ranked_valhist_filename, prop_filename, ranked_deed_filename, taxhist_filename: 
        all strings ("TaxHist36061.parquet")
    - input_dir: str, path to directory where the file exists (and where all other files will be saved to).
        The filename above should be included within a subdirectory called "raw/".
    
    Returns: Nothing. Saves parquet file to a parquet subdirectory within the input_dir.
    - merged parquet files (found withing the "unified/" subdirectory).
    '''
    #read in parquet as lazy Dataframes
    logging.info(f"Reading in parquet files to merge...")
    ranked_valhist = pl.scan_parquet(Path(input_dir+"/staging/"+ranked_valhist_filename), low_memory=True)
    prop = pl.scan_parquet(Path(input_dir+"/staging/"+prop_filename), 
        low_memory=True).drop(
            ['PropertyClassID','FATimeStamp','SitusGeoStatusCode','FIPS_SitusCensusTract','AssessmentUsed']
        )
    ranked_deed = pl.scan_parquet(Path(input_dir+"/staging/"+ranked_deed_filename), low_memory=True)
    taxhist = pl.scan_parquet(Path(input_dir+"/staging/"+taxhist_filename), low_memory=True)

    #set output path
    output_filepath = Path(input_dir+"/unified/merged.parquet")

    logging.info(f"Joining ranked val hist to prop, ranked_deed, and taxhist...")
    # https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.join.html
    step1 = (ranked_valhist.join(
        # first join in the data from the annual file (prop characteristics)
        other= prop,
        how = "inner",
        on='PropertyID',
        #validate='m:1', #checks if only 1 propertyid in annual file
        # force_parallel=True 
        # second join in the data from the most recent sale of each year
        ).join(
            other=ranked_deed,
            how='inner',
            left_on=['PropertyID', 'Year'],
            right_on=['PropertyID','RecordingYear']
        ).join(
            other=taxhist,
            how='inner',
            left_on=['PropertyID', 'Year'], 
            right_on=['PropertyID','TaxYear']
        )).collect(streaming=True)

    step1.write_parquet(output_filepath, use_pyarrow=True, compression="snappy")
    step1.clear()
    
    logging.info(f"Merged parquet file completed")

def main(input_dir: str, log_file: str, annual_file_string: str, value_history_file_string: str):
    '''
    Walks step by step through the process of the ETL pipeline by: 
    - setting up the environment
    - converting each of the .txt.zip files into parquet files
    - joins all the parquet files and filters to only the observations 
        which contain assessed values and sales values
    - validates/standardizes the geographic elements using spatil join.

    Inputs:
    - input dir (str): Path to input directory, containing a raw/ 
        subdirectory with Deed, Prop, TaxHist, and ValHist .txt.zip files.
    - log_file (str): Path to a log file where all "logging.info()" 
        strings will be saved.
    - annual file string (str): either "Prop" or "Annual" depending 
        on how the prop/annual filename is saved.
    - value history file string (str): either "ValHist" or "ValueHistory" depending 
        on how the value history filename is saved.

    Returns: Nothing. Saves new files to the following directories:
    - staging: contains intermediate parquet files for all input .txt.zip files
    - unified: contains final parquet files with all merged content
    - unzipped: (temp dir that gets deleted at the end of the script) 
        contains unzipped txt files before reading into parquet
    '''
    # set up file environment
    staging_dir = input_dir + "/" + "staging"
    unzipped_dir = input_dir + "/" + "unzipped"
    unified_dir = input_dir + "/" + "unified"
    raw_dir = input_dir + "/" + "raw"
    if not os.path.exists(staging_dir):
        os.makedirs(staging_dir)
    if not os.path.exists(unzipped_dir):
        os.makedirs(unzipped_dir)
    if not os.path.exists(unified_dir):
        os.makedirs(unified_dir)
    Path(log_file).touch()
    
    # set up logging
    logging.basicConfig(filename=Path(log_file), format='%(asctime)s:%(message)s: ', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    logging.captureWarnings(True)
    logging.info(f'Starting first american ETL for {input_dir}. Memory usage {mem_profile()}')

    # make sure there is a "raw" dir with files
    if not os.path.exists(raw_dir):
        logging.info(f'No raw subdir in the input dir. Stopping ETL.')
        sys.exit()

    # get all files within the directory and sort into a dictionary
    logging.info("Collecting all files in input directory...")
    filenames = [file for file in os.listdir(raw_dir) if os.path.isfile(os.path.join(raw_dir, file))]
    sorted_filenames = {}
    for file_type in [annual_file_string, "Deed", "TaxHist", value_history_file_string]:
        sorted_filenames[file_type] = [filename for filename in filenames if file_type in filename]
    logging.info(f'Files to process: {sorted_filenames}')

    # check to make sure there is one of each of 4 files to continue
    for type, list in sorted_filenames.items():
        if len(list) < 1:
            logging.info(f'Raw subdir does not have a {type} file. Stopping ETL.')
            sys.exit()

    # convert each file to parquet
    logging.info("Looping through all files...")
    for type, list in sorted_filenames.items():
        if type == "Deed":
            for filename in list:
                logging.info(f'Processing {filename}. Memory usage {mem_profile()}')
                convert_sales(filename, input_dir)
        if type == annual_file_string:
            for filename in list:
                logging.info(f'Processing {filename}. Memory usage {mem_profile()}')
                convert_prop(filename, input_dir)
        if type == "TaxHist":
            for filename in list:
                logging.info(f'Processing {filename}. Memory usage {mem_profile()}')
                convert_taxhist(filename, input_dir)
        if type == value_history_file_string:
            for filename in list:
                logging.info(f'Processing {filename}. Memory usage {mem_profile()}')
                convert_valhist(filename, input_dir)

    # get all files within the directory and sort into a dictionary
    logging.info("Collecting all files in staging directory...")
    filenames = [file for file in os.listdir(staging_dir) if os.path.isfile(os.path.join(staging_dir, file))]
    sorted_filenames = {}
    for file_type in [annual_file_string, "Deed", "TaxHist", f"{value_history_file_string}"]:
        sorted_filenames[file_type] = [filename for filename in filenames if file_type in filename]
    logging.info(f'Relevant files in staging: {sorted_filenames}')

    # check to make sure there is one of each of 4 files to continue
    for type, list in sorted_filenames.items():
        if len(list) < 1:
            logging.info(f'Raw subdir does not have a {type} file. Stopping ETL.')
            sys.exit()

    #@TODO: generalize this to multiple files
    #assuming only one file per list
    logging.info(f'Join into unified file...')
    logging.info(f'Memory usage {mem_profile()}')

    join(input_dir=input_dir, 
        ranked_valhist_filename=sorted_filenames[f'{value_history_file_string}'][0], 
        prop_filename=sorted_filenames[annual_file_string][0], 
        ranked_deed_filename=sorted_filenames['Deed'][0], 
        taxhist_filename=sorted_filenames['TaxHist'][0])
    logging.info(f'Join complete.')

    #clean up
    #delete empty unzipped folder
    logging.info(f'Cleaning directory...')
    os.rmdir(unzipped_dir)

    logging.info(f'Memory usage {mem_profile()}')
    logging.info("Done.")

def setup(args=None):
    '''
    Parses command line arguments for script.
    '''
    parser = argparse.ArgumentParser(description='Conducts ETL pipeline to create one combined parquet file.')
    parser.add_argument('--input_dir', required=True, type=str, dest="input_dir", help="Path to input directory, containing a raw/ subdirectory with Deed, Prop/Annual, TaxHist, and ValHist .txt.zip files.")
    parser.add_argument('--log_file', required=True, type=str, dest="log_file", help="Path to log file.")
    parser.add_argument('--annual_file_string', required=True, type=str, dest="annual_file_string", help="Substring used to determine which file in the dir is the annual file (either Prop or Annual)")
    parser.add_argument('--value_history_file_string', required=True, type=str, dest="value_history_file_string", help="Substring used to determine which file in the dir is the value history file (either ValHist or ValueHistory)")
    return parser.parse_args(args)

if __name__ == "__main__":
    main(**vars(setup()))

# example of how to run the above script (see fa-etl.sh or fa-etl.sbatch)
# python fa-etl.py --input_dir $input_dir_arg --log_file $log_file_arg --annual_file_string $annual_file_string_arg --value_history_file_string $value_history_file_string_arg