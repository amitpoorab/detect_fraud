from sqlalchemy import create_engine
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.functions import *

def set_config()  :

    """
    parser = SafeConfigParser()
    parser.read(var_dict['configFile'])
    mysql_meta_host     = parser.get(job_name, 'mysql_meta_host') 
    mysql_meta_port     = int(parser.get(job_name, 'mysql_meta_port'))
    mysql_meta_schema   = parser.get(job_name, 'mysql_meta_schema')
    mysql_meta_user     = parser.get(job_name, 'mysql_meta_user')
    mysql_meta_password = parser.get(job_name, 'mysql_meta_password')
    """
    config = {}
    
    host     = 'db' 
    port     = 3306
    schema   = 'fraud'
    user     = 'test'
    password = 'test'

    
    print ('mysql+pymysql://%s:%s@%s:%d/%s?local_infile=1' % (user,password,host,port,schema))
    
    config['mysql_meta_engine'] = create_engine(
          'mysql+pymysql://%s:%s@%s:%d/%s?local_infile=1' % (user,password,host, port,schema)
          ).connect().connection  

    config['mysql_meta_cursor'] = config['mysql_meta_engine'].cursor()

    config['jdbcurl'] = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(host,port,schema,user,password)

    return config

def load_fraud_data(config):
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
    
    #print (data)   
    insert_statement = """ INSERT INTO frauds (credit_card_number,ipv4,status ) VALUES {}
    				   """.format(','.join(data))
  
    try:
        config['mysql_meta_cursor'].execute(insert_statement)
        config['mysql_meta_engine'].commit()  
    except Exception as exception:
        config['mysql_meta_engine'].rollback()

    sql = ''' SELECT * FROM frauds '''

    fraud_df = pd.read_sql(sql, config['mysql_meta_engine'])

    print(len(fraud_df))
    print(fraud_df)
    fraud_df.to_csv("outout.csv")

def read_transaction_data(config):

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    spark = SparkSession \
        .builder \
        .appName('detect_fraud') \
        .config('spark.driver.extraClassPath',
                'postgresql-42.2.5.jar') \
        .getOrCreate()

    bakery_schema = StructType([
        StructField('credit_card_number', StringType(), True),
        StructField('ipv4', StringType(), True),
        StructField('state', StringType(), True)
    ])

    df3 = spark.read \
        .format('csv') \
        .option('header', 'true') \
        .load('transactions_dataset', schema=bakery_schema)

    #print(df3.show())

    """ Assumption : I am taking each values as string even those have the 
    """
    maestro = ['5018', '5020', '5038', '56##']
    mastercard = ['51', '52', '54', '55', '222%']
    visa = ['4']
    amex = ['34', '37']
    discover = ['6011', '65']
    diners = ['300', '301', '304', '305', '36', '38']
    jcb16 = ['35']
    jcb15 = ['2131', '1800']

    card_list = [maestro,mastercard,visa,amex,discover,diners,jcb16,jcb15]
    prefix_length_map  = {}
    for lst in card_list:
        for prefix in lst:
            length = len(prefix)
            if length in prefix_length_map:
                prefix_length_map[length].append(prefix)
            else:
                prefix_length_map[length] = [prefix]

    print(prefix_length_map)

    
    def is_valid_card(x):
        for length,value in prefix_length_map.items():
            if x[:length] in value:
                return True 
        return False
    
    lines = spark.sparkContext.textFile("transactions_dataset")
    mlines = lines.map(lambda x : x.split(',')).map(lambda x: [x[0],x[1],x[2],is_valid_card(x[0])] )
    print(mlines.take(10))
    sanitized_transactions = mlines.filter(lambda x: x[-1])
    print(sanitized_transactions.take(10))
    print(mlines.count())
    print(sanitized_transactions.count())  
    sanitized_transactions_df = sanitized_transactions.toDF(["credit_card_number", "ipv4", "state", "is_valid"])

    ## fetching fraud transaction from Mysql DB

    fraud_data = spark.read.format('jdbc').options(driver = 'com.mysql.jdbc.Driver',url=config['jdbcurl'], dbtable='frauds' ).load()
    print(fraud_data.show())

    """
    ### Part 3 
    ## Assumptions: 1. some records in fraud data doesn't contain state so assuming if credit_card_number 
                    and ipv4 is matching it is a fraud record.

    
    """

    ## a.fraudulent transactions :

    sdf = sanitized_transactions_df.alias('sdf')
    fdf = fraud_data.alias('fdf')

    fraud_transactions = sdf.join(fdf, sdf.credit_card_number == fdf.credit_card_number).select('sdf.*')
    print("""############# Priting fraud_TX
           ############""")
    print(fraud_transactions.show())
    print("""############# Priting fraud_TX conunt
           ############""")
    print(fraud_transactions.count())

    ## b. fraudulent transactions by state

    ft_by_state = fraud_transactions.groupBy('credit_card_number','ipv4').count()
    print("""############# Priting fraud_TX by state""")
    print(ft_by_state.show())
    print(ft_by_state.count())

    ##c.fraudulent transactions per card vendor


def main():
 
    config = set_config()
    print(config['mysql_meta_engine'])
    #load_fraud_data(config)

    read_transaction_data(config)

if __name__ == '__main__':
    main()