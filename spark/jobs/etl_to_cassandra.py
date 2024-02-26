from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType,StructField,IntegerType
from pyspark.sql.functions import from_json,col,concat_ws,date_format,from_utc_timestamp

def create_keyspace(cass_conn):
    cass_conn.execute("""
        CREATE KEYSPACE IF NOT EXISTS stream
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    
def create_table(cass_conn):
    cass_conn.execute("""
        CREATE TABLE IF NOT EXISTS stream.users(
            id TEXT,
            fullname TEXT,
            gender TEXT,
            date_of_birth TEXT,
            age SMALLINT,
            email TEXT,
            phone TEXT,
            cell TEXT,
            address TEXT,
            postcode INT,
            lattitude TEXT,
            longitude TEXT,
            picture_url TEXT,
            registered_date TEXT,
            uuid TEXT  PRIMARY KEY,
            username TEXT,
            password TEXT,
            nat TEXT)
    """)

def transform_data(spark_df):
    
    spark_df = spark_df.withColumn('fullname',concat_ws(" ",col("name.first"),col("name.last")))
    
    spark_df = spark_df.withColumn('address',concat_ws(",",col("location.street.number"),col("location.street.name"),col("location.city"),col("location.state"),col("location.country")))
    
    spark_df = spark_df.withColumn('postcode',col('location.postcode'))
    
    spark_df = spark_df.withColumn('lattitude',col("location.coordinates.latitude"))
    
    spark_df = spark_df.withColumn('longitude',col("location.coordinates.longitude"))
    
    spark_df = spark_df.withColumn('id',concat_ws("-",col("id.name"),col("id.value")))
  
    spark_df = spark_df.withColumn('picture_url',col("picture.large"))
    
    spark_df = spark_df.withColumn("date_of_birth",date_format(from_utc_timestamp(col("dob.date"),"UTC"),"MM-dd-yyyy"))
    
    spark_df = spark_df.withColumn("age",col("dob.age"))

    spark_df = spark_df.withColumn("uuid",col("login.uuid"))
    
    spark_df = spark_df.withColumn("password",col("login.password"))
    
    spark_df = spark_df.withColumn("username",col("login.username"))
    
    spark_df = spark_df.withColumn("registered_date",date_format(from_utc_timestamp(col("registered.date"),"UTC"),"MM-dd-yyyy"))
    
    spark_df = spark_df.drop("name","location","login","dob","picture","registered")
    
    return spark_df

def create_schema(): 
    schema = StructType([
        StructField("gender", StringType(), nullable=False),
        StructField("name", StructType([
            StructField("title", StringType(), nullable=True),
            StructField("first", StringType(), nullable=True),
            StructField("last", StringType(), nullable=True),
        ])),
        StructField("location", StructType([
            StructField("street", StructType([
                StructField("number", IntegerType(), nullable=True),
                StructField("name", StringType(), nullable=True),
            ])),
            StructField("city", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
            StructField("postcode", IntegerType(), nullable=True),
            StructField("coordinates", StructType([
                StructField("latitude", StringType(), nullable=True),
                StructField("longitude", StringType(), nullable=True),
            ])),
            StructField("timezone", StructType([
                StructField("offset", StringType(), nullable=True),
                StructField("description", StringType(), nullable=True),
            ])),
        ])),
        StructField("email", StringType(), nullable=True),
        StructField("login", StructType([
            StructField("uuid", StringType(), nullable=True),
            StructField("username", StringType(), nullable=True),
            StructField("password", StringType(), nullable=True),
            StructField("salt", StringType(), nullable=True),
            StructField("md5", StringType(), nullable=True),
            StructField("sha1", StringType(), nullable=True),
            StructField("sha256", StringType(), nullable=True),
        ])),
        StructField("dob", StructType([
            StructField("date", StringType(), nullable=True),
            StructField("age", IntegerType(), nullable=True),
        ])),
        StructField("registered", StructType([
            StructField("date", StringType(), nullable=True),
            StructField("age", IntegerType(), nullable=True),
        ])),
        StructField("phone", StringType(), nullable=True),
        StructField("cell", StringType(), nullable=True),
        StructField("id", StructType([
            StructField("name", StringType(), nullable=True),
            StructField("value", StringType(), nullable=True),
        ])),
        StructField("picture", StructType([
            StructField("large", StringType(), nullable=True),
            StructField("medium", StringType(), nullable=True),
            StructField("thumbnail", StringType(), nullable=True),
        ])),
        StructField("nat", StringType(), nullable=True),
    ])
    return schema

def connect_to_cassandra():
    cas_conn = None
    try:
        cluster = Cluster(['localhost'])

        cas_conn = cluster.connect()
        
        print("Cassandra connection created successfully!")  
    except Exception as e:
        print("Cassandra connection had error: ",e)
    
    return cas_conn

def connect_to_kafka(spark_conn):
    kafka_data = None
    try:
        kafka_data = spark_conn.readStream\
                            .format("kafka")\
                            .option("kafka.bootstrap.servers","localhost:9092")\
                            .option("subscribe", "user_created")\
                            .option("startingOffsets","earliest")\
                            .load()
        print("Kafka dataframe create succesfully")
    except Exception as e:
        print("Something went wrong: ",e)
        
    return kafka_data
    
def create_spark_connection():
    conn = None
    try:
        conn = SparkSession.builder\
                .appName('LoadtoPostgre')\
                .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1')\
                .config('spark.cassandra.connection.host', 'localhost') \
                .getOrCreate()
        
        conn.conf.set("spark.sql.adaptive.enabled", "false")
              
        print("Spark connection created successfully!")
    except Exception as e:
        print(f"Couldn't create the spark session due to exception {e}")  
    
    return conn   

def get_data_from_kafka(kafka_data):
    schema = create_schema()
    personal_strDF = kafka_data.selectExpr("CAST(value as STRING)")
    personal_df = personal_strDF.select(from_json(col("value"),schema).alias("data")).select("data.*")
    return personal_df

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        kafka_data = connect_to_kafka(spark_conn)
        spark_df = get_data_from_kafka(kafka_data)
        transformed_df = transform_data(spark_df)
        
        cas_conn = connect_to_cassandra()
        
        if cas_conn is not None:
            create_keyspace(cas_conn)
            create_table(cas_conn)
            print("Stream starting..............")
            print(transformed_df.printSchema())
            streaming = transformed_df.writeStream.format("org.apache.spark.sql.cassandra")\
                        .option("checkpointLocation", '/tmp/check_point/')\
                        .option('keyspace', 'stream')\
                        .option('table', 'users')\
                        .start()\
                        .awaitTermination()