package cn.imooc.bigdata.sparkestag.etl;

import cn.imooc.bigdata.sparkestag.support.date.DateStyle;
import cn.imooc.bigdata.sparkestag.support.date.DateUtil;
import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Date;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 环比指标实现(wow: week on week):订单会员增长
 */
public class WowEtl {
    // 创建spark session
    public static SparkSession init() {
        SparkSession session = SparkSession.builder().appName("member etl")
                .master("local[*]").enableHiveSupport()   // 用spark操作hive
                .getOrCreate();
        return session;
    }

    /**
     * 注册量周环比wow(week on week)
     * @param session
     */
    public static List<Reg> regCount(SparkSession session) {
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER,30); //最大时间
        // 把localDate转换为java util的Date类型
        Date nowDaySeven = Date.from(now.atStartOfDay(zoneId).toInstant());
        Date nowDayone = DateUtil.addDay(nowDaySeven,-7);
        // 上周
        Date lastDaySeven = DateUtil.addDay(nowDayone,-7);

        // 实际需要统计lastDaySeven到nowDaySeven
        String sql = "select date_format(create_time, 'yyyy-MM-dd') as day, " +
                " count(id) as regCount from i_member.t_member where create_time >= '%s' " +
                " and create_time < '%s' group by date_format(create_time, 'yyyy-MM-dd')";
        sql = String.format(sql,DateUtil.DateToString(lastDaySeven, DateStyle.YYYY_MM_DD_HH_MM_SS),
                DateUtil.DateToString(nowDaySeven, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(sql);
        List<String> list = dataset.toJSON().collectAsList();
        List<Reg> collect = list.stream().map(str-> JSON.parseObject(str, Reg.class)).collect(Collectors.toList());
        return collect;
    }

    public static List<Order> orderCount(SparkSession session) {
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER,30); //最大时间
        // 把localDate转换为java util的Date类型
        Date nowDaySeven = Date.from(now.atStartOfDay(zoneId).toInstant());
        Date nowDayone = DateUtil.addDay(nowDaySeven,-7);
        // 上周
        Date lastDaySeven = DateUtil.addDay(nowDayone,-7);

        // 查i_order.t_order下单量
        String sql = "select date_format(create_time, 'yyyy-MM-dd') as day," +
                " count(order_id) as orderCount from i_order.t_order where create_time >='%s' and create_time < '%s' " +
                " group by date_format(create_time,'yyyy-MM-dd')";
        sql = String.format(sql,DateUtil.DateToString(lastDaySeven, DateStyle.YYYY_MM_DD_HH_MM_SS),
                DateUtil.DateToString(nowDaySeven, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(sql);
        List<String> list = dataset.toJSON().collectAsList();
        List<Order> collect = list.stream().map(str-> JSON.parseObject(str, Order.class)).collect(Collectors.toList());
        return collect;
    }

    public static void main(String[] args) {
        SparkSession se = init();
        List<Reg> regs = regCount(se);
        List<Order> orders = orderCount(se);
        System.out.println("========================" + regs);
        System.out.println("========================" + orders);
    }

    @Data
    static class Reg {
        private String day; //日期
        private Integer regCount; //当日注册量
    }

    @Data
    static class Order {
        private String day; //日期
        private Integer orderCount; //当日订单量
    }
}
