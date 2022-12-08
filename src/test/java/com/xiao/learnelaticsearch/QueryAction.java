package com.xiao.learnelaticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.alibaba.fastjson.JSON;
import com.xiao.learnelaticsearch.entity.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.elasticsearch.client.elc.QueryBuilders;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;


@Slf4j
public class QueryAction {

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
     * 条件查询
     * age=3的数据
     */
    @Test
    void query() throws IOException {
        SearchResponse<User> search = client.search(s -> s
                        .index("user")
                        .query(q -> q
                                .match(t -> t
                                        .field("age")
                                        .query("3"))),
                User.class);
        log.info("{}", search.hits().hits());
    }

    /**
     * 查询全部
     */

    @Test
    void queryAll() throws IOException {
        SearchResponse<User> response = client.search(
                s -> s.index("user").query(
                        q -> q.matchAll(QueryBuilders.matchAllQuery())
                ).size(100), User.class);
        log.info(">>> QueryAction <<< 时间 {}", response.took());
        response.hits().hits().forEach(userHit -> log.info(">>> User{} <<< {}", userHit.source().getAge(), userHit.source()));
    }

    /**
     * 分页查询
     */
    @Test
    void queryPage() throws IOException {
        SearchResponse<User> response = client.search(
                s -> s.index("user").query(
                        q -> q.matchAll(QueryBuilders.matchAllQuery())
                ).from(0).size(10), User.class);
        log.info(">>> QueryAction <<< 时间 {}", response.took());

        response.hits().hits().forEach(userHit -> log.info(">>> User{} <<< {}", userHit.source().getAge(), userHit.source()));
    }

    /**
     * 分页查询+排序
     */
    @Test
    void queryPageOrder() throws IOException {
        SearchResponse<User> response = client.search(
                s -> s.index("user").query(
                                q -> q.matchAll(QueryBuilders.matchAllQuery()))
                        .source(sour -> sour.filter(ft -> ft.excludes("name").includes("age")))
                        .from(0)
                        .size(10)
                        .sort(sr -> sr
                                .field(f -> f
                                        .field("age")
                                        .order(SortOrder.Desc))), User.class);


        log.info(">>> QueryAction <<< 时间 {}", response.took());

        response.hits().hits().forEach(userHit -> log.info(">>> User{} <<< {}", userHit.source().getAge(), userHit.source()));
    }

    /**
     * 组合查询 1
     */
    @Test
    void combinationQuery() throws IOException {
        //查询age =1 并且 sex= 女 的人
        Query queryAsQuery1 = QueryBuilders.matchQueryAsQuery("age", "1", null, null);
        Query queryAsQuery2 = QueryBuilders.matchQueryAsQuery("sex", "女", null, null);
        SearchResponse<User> response = client
                .search(ser -> ser.index("user")
                        .query(qur -> qur.bool(bl -> bl.must(queryAsQuery1, queryAsQuery2)
                        )), User.class);
        response.hits().hits().forEach(userHit -> log.info(">>> User{} <<< {}", userHit.source().getAge(), userHit.source()));
    }


    /**
     * 组合查询 1
     */
    @Test
    void combinationQuery1() throws IOException {
        SearchResponse<User> search = client
                .search(ser -> ser.index("user")
                                .query(q -> q.bool(bl ->
                                        bl.should(
                                                QueryBuilders.matchQueryAsQuery("age", "30", null, null),
                                                QueryBuilders.matchQueryAsQuery("name", "21", null, null)
                                        )
                                ))
                        , User.class);

        search.hits().hits().forEach(userHit -> log.info(">>> User{} <<< {}", userHit.source().getAge(), userHit.source()));

    }

    /**
     * 范围查询
     */
    @Test
    void scopeQuery() throws IOException {
        SearchResponse<User> search = client
                .search(ser -> ser.index("material")
                                .query(qe -> qe.range(rg -> rg
                                        .field("size")
                                        .gte(JsonData.fromJson("30"))
                                        .lt(JsonData.fromJson("50"))))
                                .size(100)
                        , User.class);
        search.hits().hits().forEach(userHit -> log.info(">>> User{} <<< {}", userHit.source().getAge(), userHit.source()));
    }

    /**
     * 模糊查询
     */

    @Test
    void vagueQuery() throws IOException {
        SearchResponse<User> response = client.search(ser -> ser.index("user")
                        .query(qe -> qe.fuzzy(fz -> fz.field("name").value("肖杰").fuzziness(Fuzziness.ONE.asString())))
                , User.class);

        response.hits().hits().forEach(userHit -> log.info(">>> User{} <<< {}", userHit.source().getAge(), userHit.source()));
    }

    /**
     * 高亮查询
     */
    @Test
    void highLightQuery() throws IOException {
        SearchResponse<User> response = client.search(ser -> ser.index("user")
                        .query(qe -> qe.match(mt -> mt.field("name").query("肖杰")))
                        .highlight(hig -> hig.fields("name", hg -> hg.preTags("<div>").postTags("</div>")))

                , User.class);
        log.info(">>> QueryAction <<< {}", response.hits());
        response.hits().hits().forEach(userHit -> log.info(">>> User{} <<< {}", userHit.source().getAge(), userHit.source()));
    }

    /**
     * 聚合查询
     *
     * @throws IOException
     */
    @Test
    void polymerizationQuery() throws IOException {
        SearchResponse<User> response = client.search(ser -> ser.index("user").query(qry -> qry.matchAll(QueryBuilders.matchAllQuery()))
                        .size(0)
                        .aggregations("maxAge", a -> a.max(mx -> mx.field("age")))
                , User.class);
        log.info(">>> QueryAction <<< {}", response.aggregations().get("maxAge").max());
    }

    /**
     * 分组查询
     */
    @Test
    void groupQuery() throws IOException {
        SearchResponse<User> response = client.search(ser -> ser.index("user")
                        .query(qry -> qry.matchAll(QueryBuilders.matchAllQuery()))
                        .size(0)
                        .aggregations("sex_group", group -> group.terms(tm -> tm.field("sex")))
                , User.class);
        log.info(">>> QueryAction <<< {}", response.aggregations().get("sex_group"));
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
