package com.bd.utils.spark.drivers.elasticsearch;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.cloud.autoscaling.Row;
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

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;

public class ESRetriever {



    public List<String> esLookup(String query, String type, int size) throws Exception {

//        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        String keystorePassword= "ks";
//        final SSLContext sslContext = SSLContexts.custom()
//                .loadTrustMaterial(new File("/tmp/elastic-certificates.p12"), keystorePassword.toCharArray(),
//                        new TrustSelfSignedStrategy())
//                .build();
//        RestClientBuilder builder =RestClient.builder(new HttpHost("localhost", 8633, "https")).setHttpClientConfigCallback(  httpAsyncClientBuilder ->
//                httpAsyncClientBuilder.setSSLHostnameVerifier (new NoopHostnameVerifier()).setSSLContext(sslContext));
//        final RestHighLevelClient esClient = new RestHighLevelClient(builder);


        RestClientBuilder builder = RestClient.builder(HttpHost.create("https://localhost")).setPathPrefix("/es-service");

        RestHighLevelClient esClient = new RestHighLevelClient(builder);
        List<String> rows = new LinkedList();


        SearchRequest searchRequest = new SearchRequest("index1");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        searchSourceBuilder.query(QueryBuilders.disMaxQuery().add(QueryBuilders.matchPhrasePrefixQuery(type, query)));


        searchSourceBuilder.size(size);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
       // System.out.println(searchResponse.getHits().getTotalHits());



        for (SearchHit hit : searchResponse.getHits().getHits()) {
           String s = hit.getSourceAsString();

           // Record r = new ObjectMapper().readValue(hit.getSourceAsString(), Record.class);
           // Row row = new Row();
          //  row.setId("" + r.getId());
            rows.add(s);
        }

        esClient.close();
        return rows;
    }


}
