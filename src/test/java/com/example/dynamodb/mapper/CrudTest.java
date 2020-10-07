package com.example.dynamodb.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.example.dynamodb.base.AbstractTestBase;
import com.example.dynamodb.mapper.model.CatalogItem;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/7
 **/
@DisplayName("高级API")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CrudTest extends AbstractTestBase {


    @Test
    @DisplayName("创建表")
    @Order(1)
    public void createTable() {
        CreateTableRequest tableRequest = mapper.generateCreateTableRequest(CatalogItem.class);
        tableRequest.setProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
        dynamoDB.createTable(tableRequest);
    }

    @Test
    @DisplayName("插入数据")
    @Order(2)
    public void createItem() {
        CatalogItem item = new CatalogItem();
        item.setId(601);
        item.setTitle("Book 601");
        item.setISBN("611-1111111111");
        item.setBookAuthors(new HashSet<>(Arrays.asList("Author1", "Author2")));
        mapper.save(item);
    }

    @Test
    @DisplayName("获取数据")
    @Order(3)
    public void getItem() {
        CatalogItem item = mapper.load(CatalogItem.class, 601);
        Assertions.assertNotNull(item);
    }

    @Test
    @DisplayName("更新数据")
    @Order(4)
    public void updateItem() {
        CatalogItem item = mapper.load(CatalogItem.class, 601);
        item.setISBN("622-2222222222");
        item.setBookAuthors(new HashSet<>(Arrays.asList("Author1", "Author3")));
        mapper.save(item);
    }

    @Test
    @DisplayName("先更新, 再获取数据(强一致性)")
    @Order(5)
    public void retrieveItem() {
        CatalogItem item = mapper.load(CatalogItem.class, 601);
        item.setISBN("633-3333333333333");
        item.setBookAuthors(new HashSet<>(Arrays.asList("Author4", "Author6")));
        mapper.save(item);

        // MapperConfig 设置为强一致性读取
        DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .build();

        CatalogItem updatedItem = mapper.load(CatalogItem.class, 601, config);
        Assertions.assertEquals(item.getISBN(), updatedItem.getISBN());
        Assertions.assertEquals(item.getBookAuthors(), updatedItem.getBookAuthors());
    }

    @Test
    @DisplayName("删除数据")
    @Order(6)
    public void deleteItem() {
        CatalogItem item = mapper.load(CatalogItem.class, 601);
        // Delete the item.
        mapper.delete(item);

        DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .build();
        // Try to retrieve deleted item.
        CatalogItem deletedItem = mapper.load(CatalogItem.class, item.getId(), config);

        Assertions.assertNull(deletedItem);
    }

    @Test
    @DisplayName("删除表")
    @Order(Order.DEFAULT)
    public void deleteTable() {
        DeleteTableRequest deleteTableRequest = mapper.generateDeleteTableRequest(CatalogItem.class);
        client.deleteTable(deleteTableRequest);
    }
}
