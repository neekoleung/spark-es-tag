package cn.imooc.bigdata.sparkestag.etl.es;

import cn.imooc.bigdata.sparkestag.support.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.io.Serializable;
import java.util.List;

/**
 * 运营Map所需数据ETL,将得到的数据存储到ES
 */
public class EsMappingEtl {

    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();
        etl(session);
    }

    private static void etl(SparkSession session) {
        Dataset<Row> member = session.sql("select id as memberId,phone,sex,member_channel as channel,mp_open_id as subOpenId," +
                " address_default_id as address,date_format(create_time,'yyyy-MM-dd') as regTime" +
                " from i_member.t_member");

        // order_commodity
        Dataset<Row> order_commodity = session.sql("select o.member_id as memberId," +
                " date_format(max(o.create_time),'yyyy-MM-dd') as orderTime," +
                " count(o.order_id) as orderCount," +
                " collect_list(DISTINCT oc.commodity_id) as favGoods, " + // collect_list() = group_concat = [1,2,3,4,5] 方便进行JSON转换
                " sum(o.pay_price) as orderMoney " +
                " from i_order.t_order as o left join i_order.t_order_commodity as oc" +
                " on o.order_id = oc.order_id group by o.member_id");

        // 用户首单免费优惠券领取情况
        Dataset<Row> freeCoupon = session.sql("select member_id as memberId, date_format(create_time,'yyyy-MM-dd') as freeCouponTime " +
                " from i_marketing.t_coupon_member where coupon_id = 1");
        // 统计coupon times
        Dataset<Row> couponTimes = session.sql("select member_id as memberId, " +
                " collect_list(date_format(create_time,'yyyy-MM-dd')) as couponTimes " +
                " from i_marketing.t_coupon_member where coupon_id != 1 group by member_id");
        // 用户总共充值金额 (付款金额 = 券面金额/2)
        Dataset<Row> chargeMoney = session.sql("select cm.member_id as memberId, sum(c.coupon_price/2) as chargeMoney " +
                " from i_marketing.t_coupon_member as cm " +
                " left join i_marketing.t_coupon as c " +
                " on cm.coupon_id = c.id where cm.coupon_channel = 1 group by cm.member_id");

        // 用户超时 Overtime
        Dataset<Row> overTime = session.sql("select (to_unix_timestamp(max(arrive_time)) - to_unix_timestamp(max(pick_time)))" +
                " as overTime, member_id as memberId" +
                " from i_operation.t_delivery group by member_id");

        // 用户反馈Feedback
        Dataset<Row> feedback = session.sql("select fb.feedback_type as feedback, fb.member_id as memberId" +
                " from i_operation.t_feedback as fb " +
                " left join (select max(id) as mid, member_id as memberId " +
                " from i_operation.t_feedback group by member_id) as t " +
                " on fb.id = t.mid");

        // 建立临时表
        member.registerTempTable("member");
        order_commodity.registerTempTable("oc");
        freeCoupon.registerTempTable("freeCoupon");
        couponTimes.registerTempTable("couponTimes");
        chargeMoney.registerTempTable("chargeMoney");
        overTime.registerTempTable("overTime");
        feedback.registerTempTable("feedback");
        // 形成宽表
        Dataset<Row> result = session.sql("select m.*, o.orderCount, o.orderTime, o.orderMoney, o.favGoods," +
                " fc.freeCouponTime, ct.couponTimes, cm.chargeMoney, ot.overTime, fb.feedback" +
                " from member as m " +
                " left join oc as o on m.memberId = o. memberId " +
                " left join freeCoupon as fc on m.memberId = fc.memberId " +
                " left join couponTimes as ct on m.memberId = ct.memberId " +
                " left join chargeMoney as cm on m.memberId = cm.memberId " +
                " left join overTime as ot on m.memberId = ot.memberId " +
                " left join feedback as fb on m.memberId = fb.memberId ");
        // 保存到ES
        JavaEsSparkSQL.saveToEs(result,"tag/_doc");
    }

    @Data
    public static class MemberTag implements Serializable {
        // 来源于i_member.t_member
        private String memberId;
        private String phone;
        private String sex;
        private String channel; // 用户注册渠道
        private String subOpenId; // 关注微信公众号的openId
        private String address;
        private String regTime; // 注册时间

        // 来源于i_order
        private Long orderCount; // 下单数量
        private String orderTime; // 最近一次下单时间 (max(create_time) i_order.t_order)
        private Double orderMoney; // 总订单金额
        private List<String> favGoods; //商品喜好

        // 来源于i_marketing
        private String freeCouponTime; // 领取首单优惠券的时间
        private List<String> couponTimes; // 其他优惠券领取时间
        private Double chargeMoney; // 总充值金额
        
        private Integer overTime; // 用户超时时间
        private Integer feedBack; // 反馈值
    }
}
