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

        MemberVo vo = new MemberVo();
        vo.setMemberSexes(memberSexes);
        vo.setMemberChannels(memberChannels);
        vo.setMemberSubs(memberSubs);
        System.out.println(JSON.toJSONString(vo));
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
    }

}
