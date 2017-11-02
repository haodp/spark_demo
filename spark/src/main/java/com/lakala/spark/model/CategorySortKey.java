package com.lakala.spark.model;

import java.math.BigDecimal;
import scala.math.Ordered;

import java.io.Serializable;

/**
 * Created by user on 2017/10/30.
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private static final long serialVersionUID = -565992549650791884L;

    private BigDecimal allmoney;
    private int cnt;

    public CategorySortKey(BigDecimal allmoney, int cnt) {
        this.allmoney = allmoney;
        this.cnt = cnt;
    }

    @Override
    public boolean $greater(CategorySortKey other) {
        if(allmoney == null || other.getAllmoney() == null){
            return true;
        }

        if(allmoney.compareTo(other.getAllmoney()) > 0) {
            return true;
        }
        if(cnt > other.getCnt()){
            return true;
        }
        return false;
    }
    @Override
    public boolean $greater$eq(CategorySortKey other) {
        if(allmoney == null || other.getAllmoney() == null){
            return true;
        }


        if(allmoney.compareTo(other.getAllmoney()) >= 0) {
            return true;
        }
        if(cnt >= other.getCnt()){
            return true;
        }
        return false;
    }
    @Override
    public boolean $less(CategorySortKey other) {
        if(allmoney == null || other.getAllmoney() == null){
            return true;
        }


        if(allmoney.compareTo(other.getAllmoney()) < 0) {
            return true;
        }
        if(cnt < other.getCnt()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey other) {
        if(allmoney == null || other.getAllmoney() == null){
            return true;
        }


        if(allmoney.compareTo(other.getAllmoney()) <= 0) {
            return true;
        }
        if(cnt <= other.getCnt()){
            return true;
        }
        return false;
    }

    @Override
    public int compare(CategorySortKey other) {
        if(allmoney == null || other.getAllmoney() == null){
            return 1;
        }


        if(allmoney.compareTo(other.getAllmoney()) > 0) {
            return 1;
        }
        if(cnt > other.getCnt()){
            return 1;
        }
        return 0;
    }

    @Override
    public int compareTo(CategorySortKey other) {
        if(allmoney == null || other.getAllmoney() == null){
            return 1;
        }
        if(allmoney.compareTo(other.getAllmoney()) >= 0) {
            return 1;
        }
        if(cnt > other.getCnt()){
            return 1;
        }
        return 0;
    }


    public BigDecimal getAllmoney() {
        return allmoney;
    }

    public void setAllmoney(BigDecimal allmoney) {
        this.allmoney = allmoney;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }
}
