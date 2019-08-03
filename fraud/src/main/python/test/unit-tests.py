from pyspark import SparkConf, SparkContext
import unittest
import logging
from pyspark.sql import SparkSession
import detect_fraud.app as app
import pandas as pd
from pandas.testing import assert_frame_equal

logging.getLogger('py4j.java_gateway').setLevel(logging.WARN)
logging.getLogger("py4j").setLevel(logging.WARN)


class PySparkTestCase(unittest.TestCase):
    """
    SparkContext being created for each test
    """
    partition_num = 1

    def setUp(self):
        # Setup a new spark context for each test
        class_name = self.__class__.__name__
        self.spark_session = SparkSession.builder \
            .master("local") \
            .appName(class_name) \
            .config("spark.ui.showConsoleProgress", False) \
            .getOrCreate()
        self.sc = self.spark_session.sparkContext

def assert_frame_equal_with_sort(results, expected, keycolumns):
    results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
    expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
    assert_frame_equal(results_sorted, expected_sorted)

class SampleTest(PySparkTestCase):
    def test_sanitized_logic(self):
        #Using the same config function to get spark context.
        config = app.set_config()
        tr_rdd = config['spark'].sparkContext.parallelize( [('30213196611688','192.168.216.212','AZ'), ('30013196611688','192.168.216.212','AZ'), 
                              ('30013196611688','192.168.216.212','AZ'),
                              ('30413196611688','192.168.216.212','AZ'),('6011321226395113','10.37.63.174','TN') ,
                               ('6012321226395113','10.37.63.174','AZ') ]).map(lambda x: list(x))

        sanitized_transactions_df = app.sanitize_transaction_data(config,tr_rdd)

        expected_pandas = pd.DataFrame({'credit_card_number':['30013196611688', '30013196611688', '30413196611688','6011321226395113'],
                                        'ipv4':['192.168.216.212','192.168.216.212', '192.168.216.212','10.37.63.174'],
                                         'state':['AZ','AZ', 'AZ','TN'],'vendor':['diners','diners','diners','discover']})

        results_pandas = sanitized_transactions_df.toPandas()
        #print(results_pandas)
        #print(expected_pandas)

        assert_frame_equal_with_sort(results_pandas, expected_pandas,['credit_card_number','ipv4'])

    def test_fraud_transaction_count(self):

        config = app.set_config()
        fraud_data = config['spark'].sparkContext.parallelize( [('30213196611688','192.168.216.212','AZ'), 
                                                            ('5455413196611688','192.168.216.212','AZ'), 
                                            ('30413196611688','192.168.216.212','AZ'),
                               ('6012321226395113','10.37.63.174','AZ') ]) \
                                .map(lambda x: list(x)).toDF(["credit_card_number","ipv4","state"])

        sanitized_transactions_df = config['spark'].sparkContext.parallelize( [('30213196611688','192.168.216.212','AZ'), 
                                            ('30413196611688','192.168.216.212','AZ'),('6011321226395113','10.37.63.174','TN') ,
                               ('6012321226395113','10.37.63.174','AZ') ]) \
                                .map(lambda x: list(x)).toDF(["credit_card_number","ipv4","state"])

        fraud_transactions = app.generate_reports_part3_0(config,fraud_data,sanitized_transactions_df)
        fraud_transactions_count = app.generate_reports_part3_a(fraud_transactions)
        

        print("fraud_transactions_count-",fraud_transactions_count)
        self.assertEqual(fraud_transactions_count,3)

    def test_count_by_state(self):

        config = app.set_config()
        fraud_data = config['spark'].sparkContext.parallelize( [('30213196611688','192.168.216.212','AZ'), 
                                                            ('5455413196611688','192.168.216.212','AZ'), 
                                            ('30413196611688','192.168.216.212','AZ'),
                               ('6012321226395113','10.37.63.174','TN') ]) \
                                .map(lambda x: list(x)).toDF(["credit_card_number","ipv4","state"])

        sanitized_transactions_df = config['spark'].sparkContext.parallelize( [('30213196611688','192.168.216.212','AZ',"mestro"), 
                                            ('30413196611688','192.168.216.212','AZ','mestro'),('6011321226395113','10.37.63.174','AZ','visa') ,
                               ('6012321226395113','10.37.63.174','TN','visa') ]) \
                                .map(lambda x: list(x)).toDF(["credit_card_number","ipv4","state","vendor"])

        fraud_transactions = app.generate_reports_part3_0(config,fraud_data,sanitized_transactions_df)
        fraud_transactions_by_state = app.generate_reports_part3_b(fraud_transactions)
        
        results_pandas = fraud_transactions_by_state.toPandas()
        expected_pandas = pd.DataFrame({'state':['AZ','TN'],'count':[2,1]})

        assert_frame_equal_with_sort(results_pandas, expected_pandas,['state'])

    def test_count_by_vendor(self):

        config = app.set_config()
        fraud_data = config['spark'].sparkContext.parallelize( [('30213196611688','192.168.216.212','AZ'), 
                                                            ('5455413196611688','192.168.216.212','AZ'), 
                                            ('30413196611688','192.168.216.212','AZ'),
                               ('6012321226395113','10.37.63.174','AZ') ]) \
                                .map(lambda x: list(x)).toDF(["credit_card_number","ipv4","state"])

        sanitized_transactions_df = config['spark'].sparkContext.parallelize( [('30213196611688','192.168.216.212','AZ',"mestro"), 
                                            ('30413196611688','192.168.216.212','AZ','mestro'),('6011321226395113','10.37.63.174','AZ','visa') ,
                               ('6012321226395113','10.37.63.174','AZ','visa') ]) \
                                .map(lambda x: list(x)).toDF(["credit_card_number","ipv4","state","vendor"])

        fraud_transactions = app.generate_reports_part3_0(config,fraud_data,sanitized_transactions_df)
        fraud_transactions_by_vendor = app.generate_reports_part3_c(fraud_transactions)
        
        results_pandas = fraud_transactions_by_vendor.toPandas()
        expected_pandas = pd.DataFrame({'vendor':['mestro','visa'],'count':[2,1]})

        print("fraud_transactions_count-",fraud_transactions_by_vendor)
        assert_frame_equal_with_sort(results_pandas, expected_pandas,['vendor'])
        

if __name__ == "__main__":
    unittest.main()


