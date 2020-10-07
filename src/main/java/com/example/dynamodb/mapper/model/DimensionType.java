package com.example.dynamodb.mapper.model;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/7
 **/
public class DimensionType {

    private String length;
    private String height;
    private String thickness;

    public String getLength() {
        return length;
    }

    public void setLength(String length) {
        this.length = length;
    }

    public String getHeight() {
        return height;
    }

    public void setHeight(String height) {
        this.height = height;
    }

    public String getThickness() {
        return thickness;
    }

    public void setThickness(String thickness) {
        this.thickness = thickness;
    }

    @Override
    public String toString() {
        return "DimensionType{" +
                "length='" + length + '\'' +
                ", height='" + height + '\'' +
                ", thickness='" + thickness + '\'' +
                '}';
    }
}
