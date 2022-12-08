package com.xiao.learnelaticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.xiao.learnelaticsearch.entity.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;

import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * 数据的插入、更新、修改、查询操作
 */
@Slf4j
public class DocumentAction {

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
     * 添加数据
     */
    @Test
    void insertData() throws IOException {
        Random random = new Random();
        User user = new User("肖杰", "男", 30, random.nextInt(1000000000));
        CreateResponse response =
                client.create(e -> e.index("user").id("22222").document(user));
        log.info("__________________________________________________________________");
        log.info("响应数据是 >>> {} 版本是 >>> {}", response.result(), response.version());
    }

    /**
     * 更新数据
     *
     * @throws IOException
     */
    @Test
    void updateData() throws IOException {
        Random random = new Random();
        User user = new User("大牛子", "女", 22, random.nextInt(100000000));
        UpdateResponse<User> response = client.update(e -> e.index("user").id("22222").doc(user), User.class);
        log.info("__________________________________________________________________");
        log.info("{} \n {}", response.result(), response.get());
    }

    /**
     * 查找数据
     *
     * @throws IOException
     */
    @Test
    void findData() throws IOException {
        GetResponse<User> response = client.get(e -> e.index("user").id("22222"), User.class);
        log.info("__________________________________________________________________");
        log.info(">>> {}", response.source());
    }

    /**
     * 条件查询
     *
     * @throws IOException
     */
    @Test
    void findAllData() throws IOException {
        SearchResponse<User> response = client.search(s -> s
                .index("user")
                .query(q -> q
                        .match(t -> t
                                .field("sex")
                                .query("男"))), User.class);
        log.info("__________________________________________________________________");
        TotalHits total = response.hits().total();
        log.info("查询数据总量 >>> {} ", total);
        boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

        if (isExactResult) {
            log.info("有 " + total.value() + " 结果");
        } else {
            log.info("有超过 " + total.value() + " 结果");
        }

        List<Hit<User>> hits = response.hits().hits();
        for (Hit<User> hit : hits) {
            User user = hit.source();
            log.info("结果是 >>> {}", user);
        }
    }

    /**
     * 批量添加数据
     *
     * @throws IOException
     */
    @Test
    void batchInsertData() throws IOException {

        BulkRequest.Builder bulkRequest = new BulkRequest.Builder();

        fetchUser().forEach(user -> {
            bulkRequest.operations(op ->
                    op.index(idx -> idx
                            .index("user")
                            .id(user.getAge().toString())
                            .document(user)));
        });
        BulkResponse response = client.bulk(bulkRequest.build());
        if (response.errors()) {
            log.info("批量处理失败");
            response.items().forEach(
                    item -> log.error("错误描述 {}", item.error().reason())
            );
        }
    }

    /**
     * 批量删除
     *
     * @throws IOException
     */
    @Test
    void batchDeleteData() throws IOException {
        BulkRequest.Builder bulkRequest = new BulkRequest.Builder();
        fetchUser().forEach(user -> {
            bulkRequest.operations(op ->
                    op.delete(d -> d
                            .index("user")
                            .id(user.getAge().toString())));
        });
        BulkResponse response = client.bulk(bulkRequest.build());
        log.info(response.took() + "");
        log.info(response.items().toString());
    }

    /**
     * 删除name=肖杰的数据
     */

    @Test
    void deleteByAge() throws IOException {
        DeleteByQueryResponse response = client.deleteByQuery(d -> d
                .index("user")
                .query(q -> q
                        .match(t -> t
                                .field("name")
                                .query("肖杰")
                        )));

        log.info(">>> DocumentAction <<< {} {}", response.total(), response.deleted());
    }

    /**
     * 生成假数据
     *
     * @return 假数据
     */
    List<User> fetchUser() {
        ArrayList<User> users = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            User user = new User();
            user.setAge(i);
            user.setName("肖杰".concat(String.valueOf(i)));
            if (i % 2 == 0) {
                user.setSex("男");
            } else {
                user.setSex("女");
            }
            users.add(user);
        }
        return users;
    }

    /**
     * 删除数据
     */
    @Test
    void deleteData() throws IOException {
        DeleteResponse response = client.delete(e -> e.index("user").id("22222"));
        log.info("__________________________________________________________________");
        log.info("删除结果 >>> {}", response.result());
    }


    @AfterEach
    void after() {
        try {
            log.info("__________________________________________________________________");
            transport.close();
            restClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
