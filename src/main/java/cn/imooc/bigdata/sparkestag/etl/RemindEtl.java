package cn.imooc.bigdata.sparkestag.etl;

import cn.imooc.bigdata.sparkestag.support.date.DateStyle;
import cn.imooc.bigdata.sparkestag.support.date.DateUtil;
import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 优惠券失效提醒：查询领券用户及券失效时间
 */
public class RemindEtl {
    // 创建spark session
    public static SparkSession init() {
        SparkSession session = SparkSession.builder().appName("member etl")
                .master("local[*]").enableHiveSupport()   // 用spark操作hive
                .getOrCreate();
        return session;
    }

    public static List<FreeReminder> freeReminderList(SparkSession session) {
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER,30); //最大时间
        // 把localDate转换为java util的Date类型
        Date nowDaySeven = Date.from(now.atStartOfDay(zoneId).toInstant());

        // 优惠券有效期为8天，需在失效后的一天的前一天提醒用户
        Date tomorrow = DateUtil.addDay(nowDaySeven,1); // 失效日期
        Date acceptDay = DateUtil.addDay(nowDaySeven,-8); // 领取日期

        String sql = "select date_format(create_time, 'yyyy-MM-dd') as day,count(member_id) as freeCount " +
                " from i_marketing.t_coupon_member where coupon_id = 1 " +
                " and coupon_channel = 2 and create_time >= '%s' " +
                " group by date_format(create_time, 'yyyy-MM-dd')";
        sql = String.format(sql,DateUtil.DateToString(acceptDay, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(sql);
        List<String> list = dataset.toJSON().collectAsList();
        List<FreeReminder> collect = list.stream().map(str-> JSON.parseObject(str,FreeReminder.class)).collect(Collectors.toList());
        return collect;
    }

    public static List<CouponReminder> couponReminders(SparkSession session) {
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER,30); //最大时间
        // 把localDate转换为java util的Date类型
        Date nowDaySeven = Date.from(now.atStartOfDay(zoneId).toInstant());

        // 优惠券有效期为8天，需在失效后的一天的前一天提醒用户
        Date tomorrow = DateUtil.addDay(nowDaySeven,1); // 失效日期
        Date acceptDay = DateUtil.addDay(nowDaySeven,-8); // 领取日期

        String sql = "select date_format(create_time, 'yyyy-MM-dd') as day,count(member_id) as couponCount " +
                " from i_marketing.t_coupon_member where coupon_id != 1 " +
                " and create_time >= '%s' " +
                " group by date_format(create_time, 'yyyy-MM-dd')";
        sql = String.format(sql,DateUtil.DateToString(acceptDay, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(sql);
        List<String> list = dataset.toJSON().collectAsList();
        List<CouponReminder> collect = list.stream().map(str-> JSON.parseObject(str,CouponReminder.class)).collect(Collectors.toList());
        return collect;
    }

    public static void main(String[] args) {
        SparkSession session = init();
        List<FreeReminder> freeReminders = freeReminderList(session);
        List<CouponReminder> couponReminders = couponReminders(session);
        System.out.println(freeReminders);
        System.out.println(couponReminders);
    }

    @Data
    static class FreeReminder {
        private String day; // 日期
        private Integer freeCount; //需要提醒使用的用户数量（首单优惠券）
    }

    @Data
    static class CouponReminder {
        private String day; // 日期
        private Integer couponCount; //需要提醒使用的用户数量（其他优惠券）
    }
}
