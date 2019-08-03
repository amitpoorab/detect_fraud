from sqlalchemy import create_engine
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import udf
import logging
import shutil
import argparse
import time

def set_config()  :

    config = {}
    parser = argparse.ArgumentParser()
    parser.add_argument("--numofcores", help="numofcores")
    args = parser.parse_args()
    config['numofcores'] = args.numofcores

    host     = 'db' 
    port     = 3306
    schema   = 'fraud'
    user     = 'test'
    password = 'test'
    num_of_cores = 6
    
    print ('mysql+pymysql://%s:%s@%s:%d/%s?local_infile=1' % (user,password,host,port,schema))
    
    config['mysql_meta_engine'] = create_engine(
          'mysql+pymysql://%s:%s@%s:%d/%s?local_infile=1' % (user,password,host, port,schema)
          ).connect().connection  

    config['mysql_meta_cursor'] = config['mysql_meta_engine'].cursor()

    config['jdbcurl'] = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(host,port,schema,user,password)

    logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)

    config['spark'] = SparkSession \
        .builder \
        .appName('detect_fraud') \
        .getOrCreate()

    config['spark'].conf.set("spark.sql.shuffle.partitions", num_of_cores)
    return config

def load_fraud_data(config):
    """Loadig data into mysql database
    """
    filepath = 'fraud_data/fraud'
    with open(filepath) as fp:
        line = fp.readline()
        cnt = 1
        data = []
        while line:
	        #print("Line {}: {}".format(cnt, line.strip()))
            line = fp.readline().strip() 
            if line:
                cnt += 1
                line_lst = line.split(",") 
                temp_str  = ",".join(["'{}'".format(string) for string in line_lst])
 
                if len(line_lst) == 2:
                    line_str = "({},NULL)".format(temp_str)
                else:
                	line_str = "({})".format(temp_str)


                data.append(line_str)
 
    truncate_statement = """ TRUNCATE TABLE frauds """  
    insert_statement = """ INSERT INTO frauds (credit_card_number,ipv4,status ) VALUES {}
    				   """.format(','.join(data))
  
    try:
        config['mysql_meta_cursor'].execute(truncate_statement)
        config['mysql_meta_cursor'].execute(insert_statement)
        config['mysql_meta_engine'].commit()  
    except Exception as exception:
        config['mysql_meta_engine'].rollback()

def read_raw_transaction_data(config):

    rdd = config['spark'].sparkContext.textFile("transactions_dataset")
    tr_rdd = rdd.map(lambda x : x.split(','))
    return tr_rdd

def sanitize_transaction_data(config,tr_rdd):
    maestro = ['5018', '5020', '5038', '56##']
    mastercard = ['51', '52', '54', '55', '222%']
    visa = ['4']
    amex = ['34', '37']
    discover = ['6011', '65']
    diners = ['300', '301', '304', '305', '36', '38']
    jcb16 = ['35']
    jcb15 = ['2131', '1800']

    card_dict = { "maestro":maestro, 
                  "mastercard":mastercard,
                  "visa":visa,
                  "amex":amex,
                  "discover":discover,
                  "diners":diners,
                  "jcb16":jcb16,
                  "jcb15":jcb15 }

    # building map card prefix length to prefix and vendor name

    prefix_length_map  = {}
    for vendor,vendor_prefix in card_dict.items():
        for prefix in vendor_prefix:
            length = len(prefix)
            if length in prefix_length_map:
                prefix_length_map[length][prefix] = vendor
            else:
                prefix_length_map[length] = {prefix: vendor}

    #broadCastDictionary = sc.broadcast(dictionary)
    # prefix_length_map = {4: {'5018': 'maestro', '5020': 'maestro'}, 3: {301:'diners'}}

    broadcastprefix_length_map = config['spark'].sparkContext.broadcast(prefix_length_map)

    def is_valid_card(x):
        for length,value in broadcastprefix_length_map.value.items():
            if x[:length] in value:
                return value[x[:length]] 
        return None
    
    mapped_rdd = tr_rdd.map(lambda x: [x[0],x[1],x[2],is_valid_card(x[0])] )
    sanitized_transactions = mapped_rdd.filter(lambda x: x[-1])

    bakery_schema = StructType([
        StructField('credit_card_number', StringType(), True),
        StructField('ipv4', StringType(), True),
        StructField('state', StringType(), True),
        StructField('vendor', StringType(), True)
    ])

    sanitized_transactions_df = sanitized_transactions.toDF(["credit_card_number", "ipv4", "state", "vendor"])
    return sanitized_transactions_df

def get_frauds_from_mysqldb(config):
    ## fetching fraud transaction from Mysql DB
    fraud_data = config['spark'].read.format('jdbc').options(driver = 'com.mysql.jdbc.Driver',url=config['jdbcurl'], dbtable='frauds' ).load()
    return fraud_data    

def generate_reports_part3_0(config,fraud_data,sanitized_transactions_df):
    """
    ### Part 3 
    ## Assumptions: 1. some records in fraud data doesn't contain state so assuming if credit_card_number 
                    and ipv4 is matching it is a fraud record. Also there are no dulicate records for a 
                    combination of credit_card_number and ipv4 which means it is ok to use these 2 colmns.
    """

    ## Question a.fraudulent transactions :

    sdf = sanitized_transactions_df.alias('sdf')
    fdf = fraud_data.alias('fdf')

    fraud_transactions = sdf.join(fdf, (sdf.credit_card_number == fdf.credit_card_number) \
                         & (sdf.ipv4 == fdf.ipv4) ).select('sdf.*')

    ## persisting as this will be used multiuple times.
    fraud_transactions.persist()
    return fraud_transactions    

def generate_reports_part3_a(fraud_transactions):
    ## fetching fraud transaction from Mysql DB
    return fraud_transactions.count()

def generate_reports_part3_b(fraud_transactions):
    ## Question b. fraudulent transactions by state
    ft_by_state = fraud_transactions.groupBy('state').count()
    return ft_by_state

def generate_reports_part3_c(fraud_transactions):
    ## Question c.fraudulent transactions per card vendor

    ft_by_vendor = fraud_transactions.groupBy('vendor').count()
    return ft_by_vendor
   
def generate_reports_part3_d(fraud_transactions):
    ## d. save the data in json file and binary files
    """ Assumptions: 1. Keeping all the transations joining with fraud (not only fraud transactions) data to for BI team to do analysis. 
    """
    def mask_card_number(x):
        return "{}*********".format(x[:-9])

    mask_card_number_df = udf(mask_card_number)

    ### I wanted to add column from both transaction file and fraud data hence wantd to use left join but
    ### as requiremnet says it only needs 1 column so using inner join
    fraud_transactions_tosave = fraud_transactions \
                                    .withColumn("masked_card_number",mask_card_number_df("credit_card_number"))

    fraud_transactions_tosave.rdd.map(lambda x : list(x)).map(lambda x: [x[4],x[1],x[2]]) \
                .map(lambda x: [x[0],x[1],x[2],sum([len(bytes(s,'utf8')) for s in x])] ) \
                .toDF(["masked_card_number","ipv4","state","nuum_of_bytes"])
                                    #.map(lambda x: list(x)) \
                                    #.map(lambda x: [x[3],x[1],x[2],sum([len(bytes(s,'utf8')) for s in x])] ) \
                                    #.toDF(["masked_card_number","ipv4","state","nuum_of_bytes"])

    print(fraud_transactions_tosave.show())
    return fraud_transactions_tosave

def generate_reports_part3_write_output(fraud_transactions_tosave):
    fraud_transactions_tosave.write \
                 .partitionBy("state") \
                 .parquet("out/fraud_output_parquet")
    ## store data in json formate
    """ Not using coalesce as it may affect performance.
    """
    fraud_transactions_tosave.write.format('json').save("out/file_name_json")

def cleanup():

    try:
        shutil.rmtree('out')
    except FileNotFoundError as ex:
        pass

def main():
 
    config = set_config()
    cleanup()

    tr_rdd = read_raw_transaction_data(config)
    sanitized_transactions_df = sanitize_transaction_data(config,tr_rdd)

    ## Loading Fraud data into Mysql database
    load_fraud_data(config)

    # loading fraud data to mysql db
    fraud_data = get_frauds_from_mysqldb(config)

    # generate required reports
    fraud_transactions = generate_reports_part3_0(config,fraud_data,sanitized_transactions_df)
    fraud_transactions_count = generate_reports_part3_a(fraud_transactions)
    fraud_transactions_by_state = generate_reports_part3_b(fraud_transactions)
    fraud_transactions_by_vendor = generate_reports_part3_c(fraud_transactions)

    # saving results to disk
    print("Fraud tx - ",fraud_transactions_count)
    fraud_transactions.coalesce(1).write.format('com.databricks.spark.csv').save('out/fraud_transactions')
    fraud_transactions_by_state.coalesce(1).write.format('com.databricks.spark.csv').save('out/fraud_transactions_by_state')
    fraud_transactions_by_vendor.coalesce(1).write.format('com.databricks.spark.csv').save('out/fraud_transactions_by_vendor')

    fraud_transactions_tosave = generate_reports_part3_d(fraud_transactions)
    generate_reports_part3_write_output(fraud_transactions_tosave)
    config['spark'].sparkContext.stop()

if __name__ == '__main__':
    main()