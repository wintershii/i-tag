package com.etl;

import lombok.Data;
import org.apache.spark.sql.SparkSession;

public class WowEtl {

    public static void main(String[] args) {

    }

    public static SparkSession init() {
        SparkSession session = SparkSession.builder().appName("member etl")
                .master("local[*]").enableHiveSupport()
                .getOrCreate();
        return session;
    }

    @Data
    static class Reg {
        private String day;
        private Integer regCount;
    }
}
