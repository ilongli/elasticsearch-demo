package com.ilongli.elasticsearchdemo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 聚合DSL语法演示
 * Created by ilongli on 2022/9/20.
 */
public class AggsTests {

    private RestHighLevelClient client;

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://42.193.255.106:19200")
        ));
    }


    @Test
    void testAggregation() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("testclient");

        // 2.准备DSL
        // 2.1 设置size
        request.source().size(0);
        // 2.2 聚合
        request.source().aggregation(
                AggregationBuilders
                        .terms("ageAgg")
                        .field("age")
                        .size(10)
        );

        // 3.发出请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 4.解析结果
        analyseResponse(response);
    }

    void analyseResponse(SearchResponse response) {
        // 4.解析结果
        Aggregations aggregations = response.getAggregations();
        // 4.1 根据聚合名称获取聚合结果
        Terms ageTerms = aggregations.get("ageAgg");
        // 4.2 获取buckets
        List<? extends Terms.Bucket> buckets = ageTerms.getBuckets();
        // 4.3 遍历
        for (Terms.Bucket bucket : buckets) {
            // 4.4 获取key
            String key = bucket.getKeyAsString();
            long docCount = bucket.getDocCount();
            System.out.println(key + " : " + docCount);
        }
    }

}
