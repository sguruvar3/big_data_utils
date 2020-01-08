package com.bd.utils.spark.drivers.elasticsearch;


import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.apache.spark.sql.AnalysisException;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.glassfish.jersey.client.ClientResponse;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;

import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;

//import sun.jvm.hotspot.ui.SAEditorPane;

public class ESResultsCounter {

    static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 8633, "https")));

    public static void main(String[] args) throws Exception {


        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();


        String keystorePassword = "rins";
        final SSLContext sslContext = SSLContexts.custom()
                .loadTrustMaterial(new File("/tmp/elastic-certificates.p12"), keystorePassword.toCharArray(),
                        new TrustSelfSignedStrategy())
                .build();


        RestClientBuilder builder = RestClient.builder(HttpHost.create("https://localhost")).setPathPrefix("/es-service");
        final RestHighLevelClient esClient = new RestHighLevelClient(builder);


        CountRequest creq = new CountRequest();
        creq.indices("index1");
        SearchRequest searchRequest = new SearchRequest("");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        creq.source(searchSourceBuilder);

        CountResponse cresp = esClient.count(creq, RequestOptions.DEFAULT);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(cresp.getCount());

        client.close();

    }
}
