import json
import numpy as np
from pyspark import SparkContext, SQLContext

INCENT_PATH = 'hdfs:///user/mrizzo/incent_df'
NON_INCENT_PATH = 'hdfs:///user/mrizzo/non_incent_df'
ALL_PATH = 'hdfs:///user/mrizzo/joined_df'
OUT_PATH = 'hdfs:///user/cyu/electronics_df'
OUT_INCENT_PATH = 'hdfs:///user/cyu/electronics_incent_df'
OUT_NON_INCENT_PATH = 'hdfs:///user/cyu/electronics_non_incent_df'

def main():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    # Load (incentivized/non incentivized) reviews of all categories
    df_all = sqlContext.read.parquet(ALL_PATH)
    df_incent = sqlContext.read.parquet(INCENT_PATH)
    df_non_incent = sqlContext.read.parquet(NON_INCENT_PATH)

    # Filter the reviews with 'Electronics' as its main_category
    df_elec = df_all \
              .filter(df_all.main_category == 'Electronics')
    df_elec_incent = df_incent \
                     .filter(df_incent.main_category == 'Electronics')
    df_elec_non_incent = df_non_incent \
                         .filter(df_non_incent.main_category == 'Electronics')
    
    # Save filtered data as parquet files
    df_elec.write.mode('overwrite').parquet(OUT_PATH)
    df_elec_incent.write.mode('overwrite').parquet(OUT_INCENT_PATH)
    df_elec_non_incent.write.mode('overwrite').parquet(OUT_NON_INCENT_PATH)


if __name__ == '__main__':
    main()
