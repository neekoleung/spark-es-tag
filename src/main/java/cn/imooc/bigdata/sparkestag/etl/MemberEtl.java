package cn.imooc.bigdata.sparkestag.etl;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 会员指标统计：性别、注册渠道、是否微信公众号用户、活跃度
 *
 */
public class MemberEtl {
    // 创建spark session
    public static SparkSession init() {
        SparkSession session = SparkSession.builder().appName("member etl")
                .master("local[*]").enableHiveSupport()   // 用spark操作hive
                .getOrCreate();
        return session;
    }

    /**
     * 会员性别ETL：分组统计不同性别的会员数量
     * @param session
     * @return
     */
    public static List<MemberSex> memberSex(SparkSession session) {
        Dataset<Row> dataset = session.sql("select sex as memberSex,count(id) as sexCount from i_member.t_member group by sex");
        List<String> list = dataset.toJSON().collectAsList();
        System.out.println(JSON.toJSONString(list));
        List<MemberSex> collect = list.stream()
                .map(str-> JSON.parseObject(str,MemberSex.class))
                .collect(Collectors.toList());
        return collect;
    }

    /**
     * 会员注册渠道ETL：分组统计不同注册渠道的会员数量
     * @param session
     * @return
     */
    public static List<MemberChannel> memberRegChannel(SparkSession session) {
        Dataset<Row> dataset = session.sql("select member_channel as memberChannel,count(id) as channelCount from i_member.t_member group by member_channel");
        List<String> list = dataset.toJSON().collectAsList();
        //System.out.println(JSON.toJSONString(list));
        List<MemberChannel> collect = list.stream()
                .map(str-> JSON.parseObject(str,MemberChannel.class))
                .collect(Collectors.toList());
        return collect;
    }

    /**
     * 会员是否关注微信公众号ETL：分组统计关注的会员数量
     * @param session
     * @return
     */
    public static List<MemberMpSub> memberMpSub(SparkSession session) {
        Dataset<Row> sub = session.sql("select count(if(mp_open_id != 'null',id,null)) as SubCount, "+
                " count(if(mp_open_id = 'null',id,null)) as unSubCount "+
                " from i_member.t_member");
        List<String> list = sub.toJSON().collectAsList();
        //System.out.println(JSON.toJSONString(list));
        List<MemberMpSub> collect = list.stream()
                .map(str-> JSON.parseObject(str,MemberMpSub.class))
                .collect(Collectors.toList());
        return collect;
    }

    /**
     * 会员活跃度
     * @param session
     * @return
     */
    public static MemberHeat memberHeat(SparkSession session) {
        // reg, complete 通过 i_member.t_member:phone = 'null'
        Dataset<Row> reg_complete = session.sql("select count(if(phone='null',id,null)) as reg, " +
                " count(if(phone!='null',id,null)) as complete " +
                " from i_member.t_member");

        // order, orderAgain 通过 i_order.t_order
        Dataset<Row> order_again = session.sql("select count(if(t.orderCount=1,t.member_id,null)) as order," +
                "count(if(t.orderCount>=2,t.member_id,null)) as orderAgain from " +
                "(select count(order_id) as orderCount, member_id from i_order.t_order group by member_id) as t");

        // coupon 通过 i_marketing.t_coupon_member(领过券的用户数量)
        Dataset<Row> coupon = session.sql("select count(distinct member_id) as coupon from i_marketing.t_coupon_member ");

        // 笛卡尔积(尽量少用，比较耗时且效率低，但是用于数据量少的情形)
        Dataset<Row> result = coupon.crossJoin(reg_complete).crossJoin(order_again);

        List<String> list = result.toJSON().collectAsList();
        List<MemberHeat> collect = list.stream().map(str->JSON.parseObject(str,MemberHeat.class)).collect(Collectors.toList());

        return collect.get(0);
    }


    public static void main(String[] args) {
        // spark session
        SparkSession session = init();
        List<MemberSex> memberSexes = memberSex(session);
        List<MemberChannel> memberChannels = memberRegChannel(session);
        List<MemberMpSub> memberMpSubs = memberMpSub(session);
        MemberHeat memberHeat = memberHeat(session);

        MemberVo vo = new MemberVo();
        vo.setMemberSexes(memberSexes);
        vo.setMemberChannels(memberChannels);
        vo.setMemberMpSubs(memberMpSubs);
        vo.setMenberHeat(memberHeat);
        System.out.println("================" + JSON.toJSONString(vo));
    }

    @Data
    static class MemberSex {
        private Integer memberSex; //用户性别
        private Integer sexCount; //该性别数量
    }

    @Data
    static class MemberChannel {
        private Integer memberChannel; //用户注册渠道
        private Integer channelCount; //该渠道对应数量
    }

    @Data
    static class MemberMpSub {
        private Integer subCount; //订阅数量
        private Integer unSubCount; //未订阅数量
    }

    @Data
    static class MemberVo {
        private List<MemberSex> memberSexes;
        private List<MemberChannel> memberChannels;
        private List<MemberMpSub> memberMpSubs;
        private MemberHeat menberHeat;
    }

    @Data
    static class MemberHeat {
        private Integer reg; //已注册
        private Integer complete; //用户信息已完善
        private Integer order; //已下单
        private Integer orderAgain; //已复购
        private Integer coupon; //已领券
    }
}
