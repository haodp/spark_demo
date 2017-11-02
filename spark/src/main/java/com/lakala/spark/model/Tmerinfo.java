package com.lakala.spark.model;


import java.io.Serializable;

/**
 * Created by user on 2017/10/30.
 */
public class Tmerinfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 商户ＩＤ */
    private String taccountid;
    /** 商户Name */
    private String mername;

    public String getTaccountid() {
        return taccountid;
    }

    public void setTaccountid(String taccountid) {
        this.taccountid = taccountid;
    }

    public String getMername() {
        return mername;
    }

    public void setMername(String mername) {
        this.mername = mername;
    }

    public Tmerinfo(String taccountid, String mername) {
        this.taccountid = taccountid;
        this.mername = mername;
    }



    @Override
    public String toString() {
        return "Tmerinfo{" +
                "taccountid='" + taccountid + '\'' +
                ", mername='" + mername + '\'' +
                '}';
    }


    @Override
    public int hashCode() {
        int result = taccountid != null ? taccountid.hashCode() : 0;
        result = 31 * result + (mername != null ? mername.hashCode() : 0);
        return result;
    }
}
