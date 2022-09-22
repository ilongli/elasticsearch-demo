package com.ilongli.elasticsearchdemo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author ilongli
 * @date 2022/9/22 16:51
 */
public class SuggestTests {

    private RestHighLevelClient client;

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://42.193.255.106:19200")
        ));
    }

    @Test
    void testSuggest() throws IOException {

        // 1.准备Request
        SearchRequest request = new SearchRequest("test");

        // 2.准备DSL
        request.source().suggest(new SuggestBuilder().addSuggestion(
                "titleSuggest",
                SuggestBuilders.completionSuggestion("title")
                        .prefix("s")
                        .skipDuplicates(true)
                        .size(10)
        ));

        // 3.发出请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 4.解析结果
        analyseResponse(response);
    }

    void analyseResponse(SearchResponse response) {
        // 4.解析结果
        Suggest suggest = response.getSuggest();
        // 4.1 根据名称获取补全结果
        CompletionSuggestion suggestion = suggest.getSuggestion("titleSuggest");
        // 4.2 获取options并遍历
        for (CompletionSuggestion.Entry.Option option : suggestion.getOptions()) {
            // 4.3 获取一个option中的text，也就是补全的词条
            String text = option.getText().string();
            System.out.println(text);
        }
    }

}
