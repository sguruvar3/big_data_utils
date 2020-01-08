package com.bd.utils.spark.drivers.solr;


import org.apache.http.HttpHost;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.MapSolrParams;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SolrRetriever {

    private String url = "http://localhost:8633";


    public List<String> lookup(String query, String type) throws Exception {
        final String solrUrl = url + "/solr";
        HttpSolrClient client = new HttpSolrClient.Builder(solrUrl)
                .withConnectionTimeout(10000)
                .withSocketTimeout(60000)
                .build();
        List<String> rows = new LinkedList();
        final Map<String, String> queryParamMap = new HashMap<String, String>();

        queryParamMap.put("q", type + ":\"" + query + "\"");

        MapSolrParams queryParams = new MapSolrParams(queryParamMap);


        final QueryResponse response = client.query("coll1", queryParams);
        final SolrDocumentList documents = response.getResults();


        StringBuffer sb = new StringBuffer();
        for (SolrDocument document : documents) {
            String s = document.getFirstValue("col1").toString();

            rows.add(s);
        }
        return rows;
    }

}
