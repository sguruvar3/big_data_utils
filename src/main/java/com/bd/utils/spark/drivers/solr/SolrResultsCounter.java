package com.bd.utils.spark.drivers.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

//import sun.jvm.hotspot.ui.SAEditorPane;

public class SolrResultsCounter {

    static HttpSolrClient solrClient = new HttpSolrClient.Builder("http://localhost:8630/solr/coll1")
            .withConnectionTimeout(10000)
            .withSocketTimeout(60000)
            .build();

    public static void main(String[] args) throws Exception {
        SolrQuery q = new SolrQuery("*:*");
        q.setRows(0);
        long c = solrClient.query(q).getResults().getNumFound();
        System.out.println(c);

    }
}
