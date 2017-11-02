package com.lakala.spark.model;

/**
 * Created by user on 2017/11/1.
 */
public class TtransactionResult implements java.io.Serializable {

    /** 交易金额 */
    private String amt;
    /** 交易笔数 */
    private String amtCnt;
    /** 交易笔数 */
    private String failCnt;
    /** 交易笔数 */
    private String failPer;
    /** 支付时产生的费用 */
    private String fee;

    public String getAmt() {
        return amt;
    }

    public void setAmt(String amt) {
        this.amt = amt;
    }

    public String getAmtCnt() {
        return amtCnt;
    }

    public void setAmtCnt(String amtCnt) {
        this.amtCnt = amtCnt;
    }

    public String getFailCnt() {
        return failCnt;
    }

    public void setFailCnt(String failCnt) {
        this.failCnt = failCnt;
    }

    public String getFailPer() {
        return failPer;
    }

    public void setFailPer(String failPer) {
        this.failPer = failPer;
    }

    public String getFee() {
        return fee;
    }

    public void setFee(String fee) {
        this.fee = fee;
    }
}
