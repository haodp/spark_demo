package com.lakala.spark.model;

import java.io.Serializable;

/**
 * Created by user on 2017/10/30.
 */
public class Torder implements Serializable {

    /** 商户ＩＤ */
    private String storeid;
    /** 交易状态 */
    private String state;
    /** 金额 */
    private String payamount;

    public String getStoreid() {
        return storeid;
    }

    public void setStoreid(String storeid) {
        this.storeid = storeid;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getPayamount() {
        return payamount;
    }

    public void setPayamount(String payamount) {
        this.payamount = payamount;
    }

    public Torder(String storeid, String state, String payamount) {
        this.storeid = storeid;
        this.state = state;
        this.payamount = payamount;
    }
}
