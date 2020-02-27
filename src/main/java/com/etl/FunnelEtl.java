package com.etl;

import com.support.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FunnelEtl {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();
        FunnelVo funnel = funnel(session);

    }

    public static FunnelVo funnel(SparkSession session) {
        //订单，复购，充值
        Dataset<Row> orderMember = session.sql("select distinct(member_id) from i_order.t_order where order_status = 2");
        Dataset<Row> orderAgainMember = session.sql("select t.member_id from" +
                " (select count(order_id) as orderCount,member_id from i_order.t_order " +
                " where order_status = 2 group by member_id) as t where t.orderCount > 1");
        Dataset<Row> charge = session.sql("select distinct(member_id) " +
                " from i_marketing.t_coupon_member where coupon_channel = 1 " +
                " where coupon_channel = 1");
        Dataset<Row> join = charge.join(orderAgainMember,
                orderAgainMember.col("member_id").equalTo(charge.col("member_id")),"inner");
        long order = orderMember.count();
        long orderAgain = orderAgainMember.count();
        long chargeCoupon = join.count();
        FunnelVo vo = new FunnelVo();
        vo.setPresent(1000L);
        vo.setClick(800L);
        vo.setAddCart(600L);
        vo.setOrder(order);
        vo.setOrderAgain(orderAgain);
        vo.setChargeCoupon(chargeCoupon);
        return vo;
    }

    @Data
    static class FunnelVo {
        private Long present;   //1000
        private Long click;     //800
        private Long addCart;   //600
        private Long order;
        private Long orderAgain;
        private Long chargeCoupon;
    }
}
