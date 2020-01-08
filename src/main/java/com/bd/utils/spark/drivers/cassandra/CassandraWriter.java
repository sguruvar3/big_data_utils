package com.bd.utils.spark.drivers.cassandra;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;


public class CassandraWriter {



    public static void main(String arp[]){
        SparkSession sparkSession = SparkSession.builder().appName("Spark-Siva")
                .master("local[*]")
                .config("spark.ui.port", "8650")
                .getOrCreate();



        String CASSANDRA_CONNECTION_SSL = "spark.cassandra.connection.ssl.";
        String CASSANDRA_CONNECTION = "spark.cassandra.connection.";
        String CASSANDRA_KEYSTORE_PATH = "/tmp/keystore.jks"; //JKS store location
        String CASSANDRA_KEYSTORE_PASSWORD = "changeit";
        String KEYSTORE_TYPE = "JKS";
        String PROTOCOL_TYPE = "TLS";
        String CLIENT_AUTH_IS_ENABLED = "true";
        String CASSANDRA_HOST = "spark.cassandra.connection.host";
        String TABLE = "table";
        String KEY_SPACE = "keyspace";


        sparkSession.conf().set(CASSANDRA_CONNECTION_SSL + "clientAuth.enabled", CLIENT_AUTH_IS_ENABLED);
        sparkSession.conf().set(CASSANDRA_CONNECTION_SSL + "protocol", PROTOCOL_TYPE);
        sparkSession.conf().set(CASSANDRA_CONNECTION_SSL + "keyStore.path", CASSANDRA_KEYSTORE_PATH);
        sparkSession.conf().set(CASSANDRA_CONNECTION_SSL + "trustStore.path", CASSANDRA_KEYSTORE_PATH);
       sparkSession.conf().set(CASSANDRA_CONNECTION_SSL + "enabled", "true");
        sparkSession.conf().set(CASSANDRA_CONNECTION_SSL + "trustStore.password", CASSANDRA_KEYSTORE_PASSWORD);
        sparkSession.conf().set(CASSANDRA_CONNECTION_SSL + "keyStore.password", CASSANDRA_KEYSTORE_PASSWORD);
        sparkSession.conf().set(CASSANDRA_CONNECTION_SSL + "keyStore.type", KEYSTORE_TYPE);
        sparkSession.conf().set("spark.cassandra.read.timeout_ms", "200000");
        sparkSession.conf().set("spark.cassandra.connection.timeout_ms", "5000");
        sparkSession.conf().set("spark.casserole.cassandra.connection.client.query_timeout_ms", "10000");
        sparkSession.conf().set("spark.casserole.cassandra.connection.enable_slow_query_logger", "true");
      sparkSession.conf().set(CASSANDRA_HOST, "localhost");
        sparkSession.conf().set("spark.cassandra.connection.port","65503");
        sparkSession.conf().set("spark.cassandra.auth.username","cassandra_user_admin");
        sparkSession.conf().set("spark.cassandra.auth.password","36F731-FADSD-4");
        Map<String, String> columnFamilyDetails = new HashMap<>();
        columnFamilyDetails.put(KEY_SPACE, "sample_key");
        columnFamilyDetails.put(TABLE, "t1");

        Dataset<Row> df = sparkSession.read().format("com.databricks.spark.csv").load("file:///tmp/1.csv");

        df.show();


        df.write().format("org.apache.spark.sql.cassandra").options(columnFamilyDetails).mode(SaveMode.Append).save();
        //df.coalesce(1).write().format("com.databricks.spark.csv").save("/tmp/out.csv");


    }
}
