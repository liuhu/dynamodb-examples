package com.example.dynamodb.mapper;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.example.dynamodb.base.AbstractTest;
import com.example.dynamodb.mapper.model.Book;
import com.example.dynamodb.mapper.model.DimensionType;
import org.junit.jupiter.api.*;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/8
 **/
public class ConvertTest extends AbstractTest {


    @BeforeEach
    public void init() {
        ListTablesResult result = client.listTables();
        if (result.getTableNames().contains("ProductCatalog")) {
            return;
        }
        CreateTableRequest request = mapper.generateCreateTableRequest(Book.class);
        request.setProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
        dynamoDB.createTable(request);
    }

    @Test
    public void saveBook() {
        DimensionType dimType = new DimensionType();
        dimType.setHeight("8.00");
        dimType.setLength("11.0");
        dimType.setThickness("1.0");

        Book book = new Book();
        book.setId(502);
        book.setTitle("Book 502");
        book.setISBN("555-5555555555");
        book.setDimensions(dimType);

        mapper.save(book);

        Book bookRetrieved = mapper.load(Book.class, 502);

        Assertions.assertEquals(book.getDimensions().getHeight(), bookRetrieved.getDimensions().getHeight());
    }
}
