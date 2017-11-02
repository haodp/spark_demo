package com.lakala.spark.model;

/**
 * Created by user on 2017/11/1.
 */
public class Ttransaction implements java.io.Serializable {


    /** 支付状态，0：未完成，1：成功，2：失败 */
    private String state;
    /** 交易金额 */
    private String amt;
    /** 支付时产生的费用 */
    private String fee;
    /** 状态件数 */
    private int cnt;


    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getAmt() {
        return amt;
    }

    public void setAmt(String amt) {
        this.amt = amt;
    }

    public String getFee() {
        return fee;
    }

    public void setFee(String fee) {
        this.fee = fee;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }

    public Ttransaction(String state, String amt, String fee, int cnt) {
        this.state = state;
        this.amt = amt;
        this.fee = fee;
        this.cnt = cnt;
    }

    public Ttransaction() {
    }
}
