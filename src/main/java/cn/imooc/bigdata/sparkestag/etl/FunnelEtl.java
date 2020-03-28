package cn.imooc.bigdata.sparkestag.etl;

import cn.imooc.bigdata.sparkestag.support.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 获取漏斗图所需数据
 */
public class FunnelEtl {

    public static FunnelVo funnel(SparkSession session) {
        // 订购人数（统计已完成订单）
        Dataset<Row> orderMember = session.sql("select distinct(member_id) from i_order.t_order where order_status=2");
        // 复购人数
        Dataset<Row> orderAgainMember = session.sql("select t.member_id as member_id from (select count(order_id) as orderCount,member_id from i_order.t_order " +
                " where order_status=2 group by member_id) as t where t.orderCount>1");
        // 储值人数
        Dataset<Row> charge = session.sql("select distinct(member_id) as member_id " +
                " from i_marketing.t_coupon_member where coupon_channel=1");
        // 需要获得基于orderAgain的charge人数
        Dataset<Row> join = charge.join(orderAgainMember, charge.col("member_id")
                .equalTo(orderAgainMember.col("member_id")), "inner");
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

    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();
        FunnelVo funnel = funnel(session);
        System.out.println(funnel);
    }

    @Data
    static class FunnelVo {
        private Long present; // 展现人数 (测试环境下无法模拟，设为固定值1000)
        private Long click;  //点击人数 (测试环境下无法模拟，设为固定值800)
        private Long addCart;  //加购人数  (测试环境下无法模拟，设为固定值600)
        private Long order;  //订购人数
        private Long orderAgain;  //复购人数
        private Long chargeCoupon;  //储值人数
    }
}
