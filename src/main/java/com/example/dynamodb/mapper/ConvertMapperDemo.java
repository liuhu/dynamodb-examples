package com.example.dynamodb.mapper;

import com.example.dynamodb.base.AbstractDemo;
import com.example.dynamodb.mapper.model.Book;
import com.example.dynamodb.mapper.model.DimensionType;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/7
 **/
public class ConvertMapperDemo extends AbstractDemo {

    private static void saveBook() {
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
        System.out.println("Book info: " + "\n" + bookRetrieved);

        bookRetrieved.getDimensions().setHeight("9.0");
        bookRetrieved.getDimensions().setLength("12.0");
        bookRetrieved.getDimensions().setThickness("2.0");

        mapper.save(bookRetrieved);

        bookRetrieved = mapper.load(Book.class, 502);
        System.out.println("Updated book info: " + "\n" + bookRetrieved);
    }
}
