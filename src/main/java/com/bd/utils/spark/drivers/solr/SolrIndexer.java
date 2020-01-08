package com.bd.utils.spark.drivers.solr;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.util.Iterator;


public class SolrIndexer {

    static HttpSolrClient solrClient = new HttpSolrClient.Builder("http://localhost:8630/solr/rins_ref_cm")
            .withConnectionTimeout(10000)
            .withSocketTimeout(60000)
            .build();

    public static void main(String[] args) throws IOException, AnalysisException {

        SparkSession sparkSession = SparkSession.builder().appName("Spark-Solr-Siva")
                .config("spark.ui.port", "8650")
                .getOrCreate();


        Dataset<Row> ds_cm = sparkSession.read().format("jdbc")
                .option("url", "jdbc:oracle:thin:user/password@host:port:sid")
                .option("dbtable", "(    select col1 from table1)")
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("fetchsize", "100000")
                .option("numPartitions", "100")
                // .option("partitionColumn", "col1")
                //.option("lowerBound", "301439216")
                //.option("upperBound","4701829040")
                .load();
        ds_cm.show();


        ds_cm.foreach((ForeachFunction<Row>) row -> {
            SolrInputDocument document = new SolrInputDocument();
            document.addField("id", row.getDecimal(0).toString());
            document.addField("col2", row.getString(1));


            solrClient.add(document);

            solrClient.commit();

        });

    }
}
