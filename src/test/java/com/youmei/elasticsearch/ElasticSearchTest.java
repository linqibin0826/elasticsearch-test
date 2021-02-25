package com.youmei.elasticsearch;

import com.youmei.elasticsearch.pojo.Item;
import com.youmei.elasticsearch.repository.ItemRepository;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilterBuilder;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SourceFilter;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
@RunWith(SpringRunner.class)
public class ElasticSearchTest {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Test
    public void createIndex(){
        // 创建索引库
        boolean index = elasticsearchTemplate.createIndex(Item.class);

        // 添加映射信息
        elasticsearchTemplate.putMapping(Item.class);
    }


    @Autowired
    private ItemRepository itemRepository;

    @Test
    public void saveDoc() {
        Item item = new Item(1L, "小米11", "手机", "小米",
                3999D, "http://www.xiaomi.com");
        // 新增一条文档
        itemRepository.save(item);
    }

    @Test
    public void saveDocs() {
        List<Item> items = new ArrayList<>();
        items.add(new Item(2L, "小米MIX3", "手机", "小米", 3299D, "http://www.xiaomi.com"));
        items.add(new Item(3L, "华为Mate40", "手机", "华为", 5499D, "http://www.huawei.com"));
        // 批量保存文档
        itemRepository.saveAll(items);
    }

    @Test
    public void findDoc() {
        Iterable<Item> items = itemRepository.findAll();
        items.forEach(System.out::println);

        // 测试接口默认实现
        List<Item> byPriceBetween = itemRepository.findByPriceBetween(3000d, 4000d);
        byPriceBetween.forEach(System.out::println);
    }

    @Test
    public void testQuery() {
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "小米电视");
        Iterable<Item> items = itemRepository.search(queryBuilder);
        items.forEach(System.out::println);
    }

    @Test
    public void testNativeQuery() {
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        // 添加分词查询
        NativeSearchQueryBuilder queryBuilder = nativeSearchQueryBuilder.withQuery(QueryBuilders.matchQuery("title", "小米电视"));
        // 添加分页信息
        queryBuilder.withPageable(PageRequest.of(0, 2));

        // 添加排序信息
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.ASC));

        // 构建
        NativeSearchQuery searchQuery = queryBuilder.build();
        // 执行搜索,返回分页结果
        Page<Item> itemPage = itemRepository.search(searchQuery);
        // 获取总条数
        long totalElements = itemPage.getTotalElements();
        // 获取总页数
        int totalPages = itemPage.getTotalPages();
        // 获取内容
        List<Item> items = itemPage.getContent();
        items.forEach(System.out::println);
    }

    @Test
    public void testAggregations() {
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        // 不查询任何结果,过滤结果集。
        FetchSourceFilterBuilder fetchSourceFilterBuilder = new FetchSourceFilterBuilder();
        SourceFilter sourceFilter = fetchSourceFilterBuilder.withIncludes(new String[]{}).build();
        nativeSearchQueryBuilder.withSourceFilter(sourceFilter);
        // 添加聚合
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("group_by_brand").field("brand");
        // 分组后添加子聚合, 即计算组内的价格平均值
        TermsAggregationBuilder subAggregationBuilder = termsAggregationBuilder.
                subAggregation(AggregationBuilders.avg("price_avg").field("price"));
        nativeSearchQueryBuilder.addAggregation(subAggregationBuilder);
        NativeSearchQuery nativeSearchQuery = nativeSearchQueryBuilder.build();
        // 执行查询
        Page<Item> itemsPage = itemRepository.search(nativeSearchQuery);
        // 先将结果转型为带聚合信息的分页
        AggregatedPage aggregatedPage = (AggregatedPage) itemsPage;
        // 因为使用的是terms, 根据词条聚合, 所以将Aggregation强转为StringTerms类型.
        StringTerms group_by_brand = (StringTerms)aggregatedPage.getAggregation("group_by_brand");
        // 取出所有buckets
        List<StringTerms.Bucket> buckets = group_by_brand.getBuckets();
        buckets.forEach(bucket -> {
            InternalAvg price_avg = bucket.getAggregations().get("price_avg");
            System.out.println(bucket.getKeyAsString());
            System.out.println(bucket.getDocCount());
            System.out.println("平均售价: " + price_avg.getValue());
        });
    }


}
