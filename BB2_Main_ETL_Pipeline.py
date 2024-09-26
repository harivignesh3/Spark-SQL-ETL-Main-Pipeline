import sys

#This sparkETLMainPipeline1.py is a Modernized/Standardized code of spark_sql_ETL_ELT_2.py (Core Essence the ETL program spark_sql_ETL_ELT_2.py)
#inline function (scope of this function is within this module)

def reord(df): #inline function
    return df.select("id","custprof","custage","custlname","custfname")


def enrich(df):  #inline function
    add_cols=df.withColumn("loaddt",current_date()).withColumn("loadts",current_timestamp())
    enrich_source=add_cols.withColumnRenamed("srcsystem","src")
    # Concat to combine/merge/melting the columns
    enrich_combine=enrich_source.withColumn("nameprof",concat("custfname",lit("is a"),"custprof")).drop("custfname")
    enrich_split=enrich_combine.withColumn("custfname",split("nameprof",' ')[0])
    enrich_reformat=enrich_split.withColumn("dtstr",col("loaddt").cast("string")).withColumn("year",year(col("loaddt"))).withColumn("curdt",concat(substring("dtstr",3,2),lit("/"),substring("dtstr",6,2))).withColumn("dtfmt",date_format("loaddt",'yyyy/MM/dd hh:mm:ss'))
    return enrich_reformat

# COMMAND ----------

def pre_wrangle(df):  #inline function
    return df.select("id","custprof","custage","src","loaddt").groupBy("custprof").agg(avg("custage").alias("avg_age")).where("avg_age>49").orderBy("custprof")

# COMMAND ----------

def prewrangle_analyse(df):   #inline function
    sampledf=df.sample(.2,10)
    summ=df.summary()
    corrval=df.corr("custage","custage")
    covval=df.cov("custage","custage")
    freqval=df.freqItems(["custprof","agegrp"],.4)
    return sampledf,summ,corrval,covval,freqval

# COMMAND ----------

def agg_data(df):
    return df.groupBy("year","agegrp","custprof").agg(max("loaddt").alias("max_loaddt"), min("loaddt").alias("min_loaddt"),avg("custage").alias("avg_custage"),mean("custage").alias("mean_age"),countDistinct("custage").alias("distinct_cnt_age")).orderBy("year", "agegrp", "custprof", ascending=[False, True, False])

# COMMAND ----------

def standardize_cols(df):  #inline function
    srcsys='Retail'
    #adding columns
    reord_added=df.withColumn("srcsystem",lit(srcsys))
    #replacement of column(s)
    replaced=reord_added.withColumn("custfname",col("custlname")) #preffered way if few columns requires drop
    # removal of columns
    change_col=replaced.drop("custlname")#preffered way if few columns requires drop
    return change_col

# COMMAND ----------

def ret_struct():  #inline function
    strt=StructType([StructField("id", IntegerType(), False),StructField("custfname", StringType(), False),StructField("custlname", StringType(), True),StructField("custage", ShortType(), True),StructField("custprof", StringType(), True)])
    return strt

# COMMAND ----------

def main(arg):
    print("define spark session object (inline code)")
    spark=get_sparksession("ETL End To End") #reuable function calling

    print("Set the logger level to error")
    spark.sparkContext.setLogLevel("ERROR")
    print("1. Data Munging")
    print("a. Raw Data Discovery (EDA) (passive) - Performing an (Data Exploration) exploratory data analysis on the raw data to identify the properties of the attributes and patterns.")
    print("b. Combining Data + Schema Evolution/Merging (Structuring)")
    print("b.1. Combining Data- Reading from a path contains multiple pattern of files")
    print("b.2. Combining Data - Reading from a multiple different paths contains multiple pattern of files")
    print("b.3. Schema Merging (Structuring) - Schema Merging data with different structures (we know the structure of both datasets)")
    print("b.4. Schema Evolution (Structuring) - source data is evolving with different structure")
    print("c.1. Validation (active)- DeDuplication")

    # Inline function calling (modularized)
    cust_struct=ret_struct() 

    #custdf_clean = spark.read.csv("file:///home/hduser/hive/data/custsmodified", mode='dropmalformed',schema=custstructtype1)  # inline code
    custdf_clean=read_data('csv',spark,arg[1],cust_struct,mod='dropMalformed')  #resuable function calling
    custdf_optimize=optimize_performance(custdf_clean,spark,4,True,True,2)
    custdf_optimize.printSchema()
    custdf_optimize.show(20,False)
    
    print("*******Dropping Duplicates of cust data**********")
    dedup_cust=dedup(custdf_optimize,["custage"],["id"],[False])  #resuable function calling
    dedup_cust.where("id=4000003").show(4)

    #Inline coding
    txns_struct=StructType([StructField("txnid",IntegerType(),False),StructField("dt",StringType()),StructField("custid",IntegerType()),StructField("amt",DoubleType()),StructField("category",StringType()),StructField("product",StringType()),StructField("city",StringType()),StructField("state",StringType()),StructField("spendby",StringType())])

    txns_clean=read_data('csv',spark,arg[2],txns_struct,mod='dropMalformed')  #resuable function calling
    txns_optimize=optimize_performance(txns_clean,spark,1,False,False,1)

    print("*******Dropping Duplicates of txns data**********")
    dedup_txns=dedup(txns_optimize,["dt","amt"],["txnid"],[False,False])  #resuable function calling
    dedup_txns.show()

    print("c.2. Data Preparation (Cleansing & Scrubbing) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies")
    print("Replace (na.replace) the key with the respective values in the columns "
           "(another way of writing Case statement)")
    prof_dict={"Therapist":"Physician","Musician":"Music Director","na":"prof not defined"}
    cc_cust=munge(dedup_cust,prof_dict,["id"],["custlname","custprof"],["custprof"],'any')  #resuable function calling
    cc_cust.show()

    print("d.1. Data Standardization (column) - Column re-order/number of columns changes (add/remove/Replacement)  to make it in a usable format")
    reord_cust=reord(cc_cust)   #inline function calling
    reord_cust.show(10,False)
    munged_cust=standardize_cols(reord_cust)   #inline function calling
    
    print("********************data munging completed (read_data() -> cleanse_data() -> optimize_data() -> munge_data() -> reord_cols() -> standardize_cols())****************")

    #TRANSFORMATION PART#
    print("***************2. Data Enrichment (values)-> Add, Rename, combine(Concat), Split, Casting of Fields, Reformat, "
          "replacement of (values in the columns) - Makes your data rich and detailed *********************")
    mun_enrich_cust=enrich(munged_cust)  #Inline function calling
    mun_enrich_cust.show()

    print("***************3. Data Customization & Processing (Business logics) -> Apply User defined functions and utils/functions/modularization/reusable functions & reusable framework creation *********************")
    print("User Defined Functions")
    #Step1: Create a Python function
    #Step2: Importing UDF spark library
    from pyspark.sql.functions import udf
    #Step3: Converting the above function using UDF into user-defined function (DSL)
    age_validation=udf(age_conversion) #Reuable Function
    #Step4: New column deriviation called age group, in the above dataframe (Using DSL)
    agegrp_mun_enrich_cust=mun_enrich_cust.withColumn("agegrp",age_validation("custage"))
    agegrp_mun_enrich_cust.show()


    print("***************4. Core Data Processing/Transformation (Level1) (Pre Wrangling) Curation -> "
          "filter, transformation, Grouping, Aggregation/Summarization, Analysis/Analytics *********************")
    print("Transformation Functions -> select, filter, sort, group, aggregation, having, transformation/analytical function, distinct...")
    prewrang_agegrp_mun_enrich_cust=pre_wrangle(agegrp_mun_enrich_cust)
    print(prewrang_agegrp_mun_enrich_cust)

    print("Filter rows and columns")
    non_child_df=fil(agegrp_mun_enrich_cust,"agegrp<>'children'").select("id","custage","loaddt","custfname","year","agegrp") #reusable function call and inline code as well
    non_child_df.show()
    for_filter_df=fil(agegrp_mun_enrich_cust,"agegrp<>'children'")
    agg_df=agg_data(for_filter_df)

    print("Case: Show me the average age of the above customer is >35 (adding having)")
    agg_filter_df=fil(agg_df,"avg_custage>35") #having clause
    agg_filter_df.show(2)

    print("Analytical Functionalities")
    #Data Random Sampling:
    #randomsample1_for_consumption3=custom_agegrp_munged_enriched_df.sample(.2,10)#Consumer (Datascientists needed for giving training to the models)
    sampledf,summarydf,corrval,covval,freqdf=prewrangle_analyse(agegrp_mun_enrich_cust)
    sampledf.show(2)
    summarydf.show(2)
    print(f"co-relation value of age is {corrval}")
    print(f"co-variance value of age is {covval}")
    freqdf.show(2)
    masked_df=mask_fields(custdf_clean, ["custlname", "custfname"], md5)

    print("***************5. Core Data Curation/Processing/Transformation (Level2) Data Wrangling -> Joins, Lookup, Lookup & Enrichment, Denormalization,Windowing, Analytical, set operations, Summarization (joined/lookup/enriched/denormalized) *********************")
    denormalized_df=custdf_clean.alias("c").join(dedup_txns.alias("t"),on=[col("c.id")==col("t.custid")],how="inner")  #Inline code
    denormalized_df.show(2)
    rno_txns=denormalized_df.select("*",row_number().over(Window.orderBy("dt")).alias("sno")) #Inline code
    rno_txns.show(2)

    print("***************6. Data Persistance (LOAD)-> Discovery, Outbound, Reports, exports, Schema migration  *********************")
    print("Random Sample DF to File System")
    sampledf.write.mode("overwrite").csv("/user/hduser/randomsample")
    print("Denormalized DF to File System")
    denormalized_df.write.mode("overwrite").json("/user/hduser/leftjoined_aggr2")
    print("Cleansed DF to Hive Table")
    custdf_clean.write.mode("overwrite").partitionBy("custprof").saveAsTable("default.cust_bb2")
    print("Masked DF to File System")
    masked_df.write.mode("overwrite").csv("file:///home/hduser/masked_cust_data")
    print("Aggregated DF to File System")
    writeRDBMSData(agg_filter_df,arg[3],'custdb','cust_age_aggr','overwrite',arg[4])
    print("Spark App1 Completed Successfully")

if __name__=="__main__":
    if(len(sys.argv)==5):
        print("parameter passed ",sys.argv)
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        from pyspark.sql.types import *
        from pyspark.sql.window import *
        from python.learnpyspark.sql.BB2_reusable_function import *
        main(sys.argv)
    else:
        print("No enough argument to continue running this program usage: ","file:///home/hduser/hive/data/custsmodified file:///home/hduser/hive/data/txns /home/hduser/connection.prop")
        exit(1)


'''
spark-submit --jars /home/hduser/install/mysql-connector-java.jar \
--py-files /home/hduser/WE43Project/WE43Project.zip \
sparkETLMainPipeline1.py file:///home/hduser/hive/data/custsmodified file:///home/hduser/hive/data/txns /home/hduser/connection.prop DEVDBCRED
'''

