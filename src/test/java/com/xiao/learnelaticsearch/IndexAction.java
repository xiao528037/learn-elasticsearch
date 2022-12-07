package com.xiao.learnelaticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * 创建、删除、查询索引
 */
@Slf4j
class IndexAction {


    RestClient restClient = null;

    ElasticsearchTransport transport = null;

    ElasticsearchClient client = null;

    /**
     * 初始化客户端
     */
    @BeforeEach
    public void createClient() {
        restClient = RestClient.builder(new HttpHost("localhost", 9200, HttpHost.DEFAULT_SCHEME_NAME)).build();
        transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        client = new ElasticsearchClient(transport);
    }

    /**
     * 创建索引
     *
     * @throws IOException
     */
    @Test
    void createIndex() throws IOException {
        CreateIndexResponse response = client.indices().create(e -> e.index("shopping"));
        log.info("创建状态 >>> {} index信息 >>> {}", response.acknowledged(), response.index());
    }

    /**
     * 查询索引信息
     *
     * @throws IOException
     */
    @Test
    void selectIndex() throws IOException {
        //查询所有索引
        GetIndexResponse response = client.indices().get(e -> e.index("*"));
        log.warn("索引信息 >>> {}", response.result().keySet());
        //查询指定索引
        GetIndexResponse shopping = client.indices().get(e -> e.index("shooping"));
        log.warn("索引信息 >>> {}", shopping.result());

    }


    /**
     * 删除索引信息
     *
     * @throws IOException
     */
    @Test
    void deleteIndex() throws IOException {
        DeleteIndexResponse response = client.indices().delete(e -> e.index("shopping"));
        log.info("索引删除 >>> {}", response.acknowledged());
    }

    @AfterEach
    void after(){
        try {
            transport.close();
            restClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

