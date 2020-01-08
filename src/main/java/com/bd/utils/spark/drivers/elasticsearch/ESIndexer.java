package com.bd.utils.spark.drivers.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.spark.api.java.function.ForeachPartitionFunction;

import org.apache.spark.sql.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;

import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Iterator;


//import sun.jvm.hotspot.ui.SAEditorPane;
public class ESIndexer{


    public static void main(String[] args) throws Exception  {

        SparkSession sparkSession = SparkSession.builder().appName("Spark-Solr-Siva-es")
                .config("spark.ui.port", "8640")
                .master("local[*]")
                .config("spark.es.nodes", "localhost")
                .config("spark.es.port", "8633")
                .getOrCreate();

        RestClientBuilder builder = RestClient.builder(HttpHost.create("https://localhost")).setPathPrefix("/es-service");

        RestHighLevelClient client = new RestHighLevelClient(builder);

        Dataset<Row> ds_cm = sparkSession.read().format("jdbc")
                .option("url", "jdbc:oracle:thin:USER/Password@host:port:sid")
                .option("dbtable", "(    select * from table  )")
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("fetchsize", "100000")
                .option("numPartitions", "100")
                .option("partitionColumn", "col1")
                .option("lowerBound", "100")
                .option("upperBound", "4701829040")
                .load();
        ds_cm.show();

        ds_cm.repartition(100);
        ds_cm.foreachPartition(new ForeachPartitionFunction<Row>() {

            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                while (iterator.hasNext()) {
                    Row row = iterator.next();

                    Record record = new Record();
                    record.setId(row.getString(0));
                    IndexRequest request = new IndexRequest("index1");
                    request.source(new ObjectMapper().writeValueAsString(record), XContentType.JSON);
                    client.index(request, RequestOptions.DEFAULT);
                }
            }
        });

        client.close();


    }


    static class Record {

        private String id;

        public void setId(String id) {
            this.id = id;
        }
    }
}
