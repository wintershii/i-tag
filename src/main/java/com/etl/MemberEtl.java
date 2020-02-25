package com.etl;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import java.util.List;
import java.util.stream.Collectors;

public class MemberEtl {

    public static void main(String[] args) {
        SparkSession session = init();

        List<MemberSex> memberSexes = memberSex(session);
        List<MemberChannel> memberChannels = memberRegChannel(session);
        List<MemberSub> memberSubs = memberMpSub(session);

        MemberHeat memberHeat = memberHeat(session);

        MemberVo vo = new MemberVo();
        vo.setMemberSexes(memberSexes);
        vo.setMemberChannels(memberChannels);
        vo.setMemberSubs(memberSubs);
        vo.setMemberHeat(memberHeat);
        System.out.println(JSON.toJSONString(vo));
    }

    /**
     * member heat etl
     * @param session
     * @return
     */
    public static MemberHeat memberHeat(SparkSession session) {
        //reg,complete,order,orderAgain,
        //reg,complete ==> i_member.t_member
        //order,orderAgain ==> i_order.t_order
        //coupon ==> i_marketing.t_coupon_member

        Dataset<Row> regComplete = session.sql("select count(if(phone='null',id,null)) as reg, " +
                " count(if(phone != 'null',id,null)) as complete " +
                " from i_member._member ");

        Dataset<Row> orderAgain = session.sql("select count(if(t.orderCount = 1),t.member_id,null) as order, " +
                " count(if(t.orderCount  >= 2),t.member_id,null) as orderAgain from " +
                " (select count(order_id) as orderCount, member_id from i_order.t_order group by member_id) as t ");

        Dataset<Row> coupon = session.sql("select count(distinct member_id) from i_marketing.t_coupon_member ");

        Dataset<Row> result = coupon.crossJoin(regComplete).crossJoin(orderAgain);
        List<String> list = result.toJSON().collectAsList();
        List<MemberHeat> collect = list.stream().map(str->JSON.parseObject(str,MemberHeat.class)).collect(Collectors.toList());
        return collect.get(0);
    }


    /**
     * member mp sub etl
     * @param session
     * @return
     */
    public static List<MemberSub> memberMpSub(SparkSession session) {
        Dataset<Row> dataset = session.sql("select count(if(mp_open_id != 'null',id,null)) as mpSubCount," +
                " count(if(mp_open_id = 'null',id,null)) as unSubCount" +
                " from i_member.t_member");


        List<String> list = dataset.toJSON().collectAsList();
        System.out.println(list);
        List<MemberSub> collect = list.stream().map(str-> JSON.parseObject(str,MemberSub.class)).collect(Collectors.toList());
        return collect;
    }


    /**
     * member reg channel etl
     * @param session
     * @return
     */
    public static List<MemberChannel> memberRegChannel(SparkSession session) {
        Dataset<Row> dataset = session.sql("select member_channel as memberChannel,count(id) as channelCount from i_member.t_member group by member_channel");
        List<String> list = dataset.toJSON().collectAsList();
        System.out.println(list);
        List<MemberChannel> collect = list.stream().map(str-> JSON.parseObject(str,MemberChannel.class)).collect(Collectors.toList());
        return collect;
    }


    /**
     * member sex etl
     * @param session
     * @return
     */
    public static List<MemberSex> memberSex(SparkSession session) {
        Dataset<Row> dataset = session.sql("select sex as memberSex,count(id) as sexCount from i_member.t_member group by sex");
        List<String> list = dataset.toJSON().collectAsList();
        System.out.println(list);
        List<MemberSex> collect = list.stream().map(str-> JSON.parseObject(str,MemberSex.class)).collect(Collectors.toList());
        return collect;
    }




    public static SparkSession init() {
        SparkSession session = SparkSession.builder().appName("member etl")
                .master("local[*]").enableHiveSupport()
                .getOrCreate();
        return session;
    }

    @Data
    static class MemberSex {
        private Integer memberSex;
        private Integer sexCount;
    }

    @Data
    static class MemberChannel {
        private Integer memberChannel;
        private Integer channelCount;
    }

    @Data
    static class MemberSub {
        private Integer subCount;
        private Integer unSubCount;
    }

    @Data
    static class MemberVo {
        private List<MemberSex> memberSexes;
        private List<MemberChannel> memberChannels;
        private List<MemberSub> memberSubs;

        private MemberHeat memberHeat;
    }

    @Data
    static class MemberHeat {
        private Integer reg;
        private Integer complete;
        private Integer order;
        private Integer orderAgain;
        private Integer coupon;
    }

}
