package com.example.dynamodb.mapper.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/7
 **/
public class DimensionTypeConverter implements DynamoDBTypeConverter<String, DimensionType> {
    @Override
    public String convert(DimensionType object) {
        DimensionType itemDimensions = (DimensionType) object;
        String dimension = null;
        try {
            if (itemDimensions != null) {
                dimension = String.format("%s x %s x %s", itemDimensions.getLength(), itemDimensions.getHeight(),
                        itemDimensions.getThickness());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return dimension;
    }

    @Override
    public DimensionType unconvert(String s) {

        DimensionType itemDimension = new DimensionType();
        try {
            if (s != null && s.length() != 0) {
                String[] data = s.split("x");
                itemDimension.setLength(data[0].trim());
                itemDimension.setHeight(data[1].trim());
                itemDimension.setThickness(data[2].trim());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return itemDimension;
    }
}
