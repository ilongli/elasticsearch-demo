package com.ilongli.elasticsearchdemo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author ilongli
 * @date 2022/8/23 15:02
 */
public class HotelIndexTest {

    private static final String MAPPING_TEMPLATE = "{\n" +
            "  \"mappings\": {\n" +
            "    \"properties\": {\n" +
            "      \"name\": {\n" +
            "        \"type\": \"text\",\n" +
            "        \"analyzer\": \"ik_smart\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
    private RestHighLevelClient client;

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://42.193.255.106:19200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }


    @Test
    public void testCreateHotelIndex() throws IOException {
        // 1.创建Request对象
        CreateIndexRequest request = new CreateIndexRequest("testclient");
        // 2.请求参数，MAPPING_TEMPLATE静态常量字符串，内容是创建索引库的DSL语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        // 3.发起请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    public void testDeleteHotelIndex() throws IOException {
        // 1.创建Request对象
        DeleteIndexRequest request = new DeleteIndexRequest("testclient");
        // 2.发起请求
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    public void testExistsHotelIndex() throws IOException {
        // 1.创建Request对象
        GetIndexRequest request = new GetIndexRequest("testclient");
        // 2.发起请求
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        // 3.输出
        System.out.println(exists);
    }


    @Test
    public void testIndexDocument() throws IOException {
        // 1.创建request对象
        IndexRequest request = new IndexRequest("testclient").id("1");
        // 2.准备JSON文档
        request.source("{\"name\": \"jack\"}", XContentType.JSON);
        // 3.发送请求
        client.index(request, RequestOptions.DEFAULT);
    }

    @Test
    public void testGetDocumentById() throws IOException {
        // 1.创建request对象
        GetRequest request = new GetRequest("testclient", "1");
        // 2.发送请求，得到结果
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 3.解析结果
        String json = response.getSourceAsString();

        System.out.println(json);
    }

    @Test
    public void testUpdateDocumentById() throws IOException {
        // 1.创建request对象
        UpdateRequest request = new UpdateRequest("testclient", "1");
        // 2.准备参数
        request.doc("name", "rose");    // 后面还有就继续加
        // 3.更新文档
        client.update(request, RequestOptions.DEFAULT);
    }

    @Test
    public void testDeleteDocumentById() throws IOException {
        // 1.创建request对象
        DeleteRequest request = new DeleteRequest("testclient", "1");
        // 2.发送请求
        client.delete(request, RequestOptions.DEFAULT);
    }

    @Test
    public void testBulk() throws IOException {
        // 1.创建Bulk请求
        BulkRequest request = new BulkRequest();
        // 2.添加要批量提交的请求
        request.add(new IndexRequest("testclient").id("1")
                .source("{\"name\": \"jack\"}", XContentType.JSON));
        request.add(new IndexRequest("testclient").id("2")
                .source("{\"name\": \"rose\"}", XContentType.JSON));
        // 3.发起bulk请求
        client.bulk(request, RequestOptions.DEFAULT);
    }

}
