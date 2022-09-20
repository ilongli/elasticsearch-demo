package com.ilongli.elasticsearchdemo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * DSL语法演示
 * Created by ilongli on 2022/9/20.
 */
public class DSLTests {

    private RestHighLevelClient client;

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://42.193.255.106:19200")
        ));
    }

    @Test
    void testMatchAll() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("testclient");
        // 2.组织DSL参数
        request.source()
                .query(QueryBuilders.matchAllQuery());
        // 3.发送请求，得到相应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        analyseResponse(response);
    }

    @Test
    void analyseResponse(SearchResponse response) {

        // 4.解析结果
        SearchHits searchHits = response.getHits();
        // 4.1 查询的总条数
        long total = searchHits.getTotalHits().value;
        // 4.2 查询的结果数组
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            // 4.3 得到source
            String json = hit.getSourceAsString();
            // 4.4 打印
            System.out.println(json);

            // 获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            // 根据字段名获取高亮结果
            HighlightField highlightField = highlightFields.get("address");
            if (Objects.nonNull(highlightField) || highlightField.getFragments().length > 0) {
                // 获取高亮值
                String address = highlightField.getFragments()[0].string();
                System.out.println("高亮：" + address);
            }

        }
    }

    @Test
    void testMatch() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("testclient");
        // 2.组织DSL参数
        request.source()
                .query(QueryBuilders.matchQuery("name", "jack"));
        // 3.发送请求，得到相应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        analyseResponse(response);
    }


    @Test
    void testBool() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("testclient");
        // 2.组织DSL参数
        // 2.1 准备BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 2.2 添加must
        boolQuery.must(QueryBuilders.termQuery("name", "jack"));
        // 2.3 添加range
        boolQuery.filter(QueryBuilders.rangeQuery("age").lte(24));
        request.source()
                .query(boolQuery);
        // 3.发送请求，得到相应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        analyseResponse(response);
    }


    @Test
    void testPageAndSort() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("testclient");
        // 2.组织DSL参数
        // 2.1 query
        request.source()
                .query(QueryBuilders.matchAllQuery());
        // 2.2 排序sort
        request.source().sort("age", SortOrder.DESC);
        // 2.3 分页 form size
        request.source().from(0).size(1);
        // 3.发送请求，得到相应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        analyseResponse(response);
    }


    @Test
    void tesHighlight() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("testclient");
        // 2.组织DSL参数
        // 2.1 query
        request.source()
                .query(QueryBuilders.matchQuery("address", "北泽"));
        // 2.2 高亮
        request.source()
                .highlighter(
                    new HighlightBuilder()
                        .field("address")
                        .requireFieldMatch(false)
                );

        // 3.发送请求，得到相应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        analyseResponse(response);
    }


}
