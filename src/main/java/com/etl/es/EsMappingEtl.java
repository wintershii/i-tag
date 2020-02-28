package com.etl.es;

import com.support.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.io.Serializable;
import java.util.List;

public class EsMappingEtl {

    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();
        etl(session);

    }

    public static void etl(SparkSession session) {
        Dataset<Row> member = session.sql("select id as memberId, phone, sex, member_channel as channel, " +
                " mp_open_id as subOpenId, address_default_id as address, " +
                " date_format(create_time,'yyyy-MM-dd') as regTime from i_member.t_member");

        Dataset<Row> orderCommodity = session.sql("select o.member_id as memberId, " +
                " date_format(max(o.create_time),'yyyy-MM-dd') as orderTime, " +
                " count(o.order_id) as orderCount, " +
                " collect_list(DISTINCT oc.commodity_id) as favGoods, " +
                " sum(o.pay_price) as orderMoney" +
                " from i_order.t_order as o left join i_order.t_order_commodity as oc " +
                " on o.order_id = oc.order_id group by o.member_id");

        Dataset<Row> freeCoupon = session.sql("select member_id as memberId, " +
                " date_format(create_time,'yyyy-MM-dd') as freeCouponTime " +
                " from i_marketing.t_coupon_member where coupon_id = 1");

        Dataset<Row> couponTimes = session.sql("select member_id as memberId, " +
                " collect_list(date_format(create_time,'yyyy-MM-dd')) as couponTimes " +
                " from i_marketing.t_coupon_member where coupon_id != 1 group by member_id");

        Dataset<Row> chargeMoney = session.sql("select cm.member_id as memberId, sum(c.coupon_price/2) as chargeMoney " +
                " from i_marketing.t_coupon_member as cm left join i_marketing.t_coupon as c " +
                " on cm.coupon_id = c.id where cm.coupon_channel = 1 group by cm.member_id");

        Dataset<Row> overTime = session.sql("select (to_unix_timestamp(max(arrive_time)) - to_unix_timestamp(max(pick_time))) as overTime, " +
                " member_id as memberId" +
                " from i_operation.t_delivery group by member_id");

        Dataset<Row> feedback = session.sql("select fb.feedback_type as feedBack, fb.member_id as memberId " +
                " from i_operation.t_feedback as fb left join (select max(id) as mid, member_id as memberId " +
                " from i_operation.t_feedback group by member_id) as t " +
                " on fb.id = t.mid");

        //注册临时表
        member.registerTempTable("member");
        orderCommodity.registerTempTable("oc");
        freeCoupon.registerTempTable("freeCoupon");
        couponTimes.registerTempTable("couponTimes");
        chargeMoney.registerTempTable("chargeMoney");
        overTime.registerTempTable("overTime");
        feedback.registerTempTable("feedback");

        //join几张表 生成宽表
        Dataset<Row> result = session.sql("select m.*, o.orderCount, o.orderTime, o.orderMoney, o.favGoods, " +
                " fb.freeCouponTime, ct.couponTimes, cm.chargeMoney, ot.overTime, f.feedBack" +
                " from member as m  " +
                " left join oc as o on m.memberId = o.memberId " +
                " left join freeCoupon as fb on m.memberId = fb.memberId " +
                " left join couponTimes as ct on m.memberId = ct.memberId " +
                " left join chargeMoney as cm on m.memberId = cm.memberId " +
                " left join overTime as ot on m.memberId = ot.memberId " +
                " left join feedback as f on m.memberId = f.memberId");

        JavaEsSparkSQL.saveToEs(result,"/tag/_doc");
    }


    @Data
    public static class MemberTag implements Serializable {
        // i_member.t_member
        private String memberId;
        private String phone;
        private String sex;
        private String channel;
        private String subOpenId;
        private String address;
        private String regTime;

        private Long orderCount;
        // max(create_time) i_order.t_order
        private String orderTime;
        private Double orderMoney;
        private List<String> favGoods;

        // i_marketing
        private String freeCouponTime;
        private List<String> couponTimes;
        private Double chargeMoney;

        private Integer overTime;
        private Integer feedBack;
    }
}
