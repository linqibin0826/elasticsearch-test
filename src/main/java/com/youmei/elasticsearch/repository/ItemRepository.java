package com.youmei.elasticsearch.repository;

import com.youmei.elasticsearch.pojo.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;


/**
 * @author Administrator
 */
public interface ItemRepository extends ElasticsearchRepository<Item, Long> {

    /**
     * Find by price between p1 and p2.
     *
     * @param p1 the p1
     * @param p2 the p2
     * @return the list
     * @author youmei
     * @since 2021 /2/25 0:39
     */
    List<Item> findByPriceBetween(Double p1, Double p2);
}
