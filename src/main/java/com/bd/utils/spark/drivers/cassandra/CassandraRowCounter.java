package com.bd.utils.spark.drivers.cassandra;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.Map;

public class CassandraRowCounter {



    public static void main(String arp[]){
//        SparkSession sparkSession = SparkSession.builder().appName("Spark-Siva")
//                .master("local[*]")
//                .config("spark.ui.port", "8650")
//                .getOrCreate();
        String CASSANDRA_CONNECTION_SSL = "spark.cassandra.connection.ssl.";
        String CASSANDRA_CONNECTION = "spark.cassandra.connection.";
        String CASSANDRA_KEYSTORE_PATH = "/tmp/keystore.jks";
        String CASSANDRA_KEYSTORE_PASSWORD = "changeit";
        String KEYSTORE_TYPE = "JKS";
        String PROTOCOL_TYPE = "TLS";
        String CLIENT_AUTH_IS_ENABLED = "true";
        String CASSANDRA_HOST = "spark.cassandra.connection.host";

        SparkConf conf1 =new SparkConf().setAppName("Siva").setMaster("local[*]")
        .set(CASSANDRA_CONNECTION_SSL + "clientAuth.enabled", CLIENT_AUTH_IS_ENABLED)
        .set(CASSANDRA_CONNECTION_SSL + "protocol", PROTOCOL_TYPE)
        .set(CASSANDRA_CONNECTION_SSL + "keyStore.path", CASSANDRA_KEYSTORE_PATH)
        .set(CASSANDRA_CONNECTION_SSL + "trustStore.path", CASSANDRA_KEYSTORE_PATH)
        .set(CASSANDRA_CONNECTION_SSL + "enabled", "true")
        .set(CASSANDRA_CONNECTION_SSL + "trustStore.password", CASSANDRA_KEYSTORE_PASSWORD)
        .set(CASSANDRA_CONNECTION_SSL + "keyStore.password", CASSANDRA_KEYSTORE_PASSWORD)
        .set(CASSANDRA_CONNECTION_SSL + "keyStore.type", KEYSTORE_TYPE)
        .set("spark.cassandra.read.timeout_ms", "200000")
        .set("spark.cassandra.connection.timeout_ms", "5000")
        .set("spark.casserole.cassandra.connection.client.query_timeout_ms", "10000")
        .set("spark.casserole.cassandra.connection.enable_slow_query_logger", "true")
        .set(CASSANDRA_HOST, "localhost")
        .set("spark.cassandra.connection.port","65503")
        .set("spark.cassandra.auth.username","cassandra_user_admin")
        .set("spark.cassandra.auth.password","2E02AC4C-A4A")
        .set("spark.cassandra.input.split.size_in_mb","67108864");



        JavaSparkContext sc= new JavaSparkContext(conf1);

       JavaRDD rdd =  CassandraJavaUtil.javaFunctions(sc).cassandraTable("sample_keyspace","table1");

       System.out.println(rdd.count());
    }
}
