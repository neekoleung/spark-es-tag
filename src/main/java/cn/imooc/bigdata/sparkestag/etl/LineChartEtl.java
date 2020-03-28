package cn.imooc.bigdata.sparkestag.etl;

import cn.imooc.bigdata.sparkestag.support.date.DateStyle;
import cn.imooc.bigdata.sparkestag.support.date.DateUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.avro.generic.GenericData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 绘制条形图所需的数据（新用户增长、总用户量、总订单量、商品GMV[订单商品总金额]）
 */
public class LineChartEtl {
    // 创建spark session
    public static SparkSession init() {
        SparkSession session = SparkSession.builder().appName("member etl")
                .master("local[*]").enableHiveSupport()   // 用spark操作hive
                .getOrCreate();
        return session;
    }

    public static List<LineVo> lineVos(SparkSession session) {
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER,30); //最大时间
        // 把localDate转换为java util的Date类型
        Date nowDay = Date.from(now.atStartOfDay(zoneId).toInstant());
        Date sevenDayBefore = DateUtil.addDay(nowDay,-8);

        // 新注册用户数regCount, 会员总数memberCount (id auto increment)
        String memberSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " count(id) as regCount,max(id) as memberCount " +
                " from i_member.t_member where create_time >= '%s' " +
                " group by date_format(create_time,'yyyy-MM-dd') order by day";
        memberSql = String.format(memberSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> memberDs = session.sql(memberSql);

        // orderCount, gmv(t_order id auto increment)
        String orderSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " max(order_id) as orderCount, sum(origin_price) as gmv" +
                " from i_order.t_order where create_time >= '%s' " +
                " group by date_format(create_time,'yyyy-MM-dd') order by day";
        orderSql = String.format(orderSql,DateUtil.DateToString(sevenDayBefore,DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> orderDs = session.sql(orderSql);

        // 通过day连接两个表 (inner join)
        Dataset<Tuple2<Row, Row>> tuple2Dataset = memberDs.joinWith(orderDs, memberDs.col("day").equalTo(orderDs.col("day")), "inner");
        List<Tuple2<Row, Row>> tuple2s = tuple2Dataset.collectAsList();
        List<LineVo> vos = new ArrayList<>();
        for (Tuple2<Row, Row> tuple2 : tuple2s) {
            JSONObject obj = new JSONObject();
            Row row1 = tuple2._1();
            Row row2 = tuple2._2();
            StructType schema = row1.schema();
            String[] strings = schema.fieldNames();
            for (String string : strings) {
                Object as = row1.getAs(string);
                obj.put(string,as);
            }
            schema = row2.schema();
            strings = schema.fieldNames();
            for (String string : strings) {
                Object as = row2.getAs(string);
                obj.put(string,as);
            }
            LineVo lineVo = obj.toJavaObject(LineVo.class);
            vos.add(lineVo); // orm转换操作
        }

        // 计算总的gmv
        String gmvTotal = "select sum(origin_price) from i_order.t_order where create_time < '%s'";
        gmvTotal = String.format(gmvTotal,DateUtil.DateToString(sevenDayBefore,DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> gmvDs = session.sql(gmvTotal);
        double gmvAll = gmvDs.collectAsList().get(0).getDouble(0);
        BigDecimal decimalGmv = BigDecimal.valueOf(gmvAll);

        List<BigDecimal> destList = new ArrayList<>();
        for (int i = 0; i < vos.size(); i++) {
            LineVo lineVo = vos.get(i);
            BigDecimal gmv = lineVo.getGmv();
            BigDecimal tmp = gmv.add(decimalGmv);
            for (int j = 0; j < i; j++) {
                // 加上前几个节点的gmv得到总的gmv
                LineVo prevDay = vos.get(j);
                tmp = tmp.add(prevDay.getGmv());
            }
            destList.add(tmp);
        }

        for (int i = 0; i < destList.size(); i++) {
            LineVo lineVo = vos.get(i);
            lineVo.setGmv(destList.get(i));
        }


        return vos;
    }

    public static void main(String[] args) {
        SparkSession init = init();
        List<LineVo> lineVos = lineVos(init);
        System.out.println(lineVos);
    }

    @Data
    static class LineVo {
        private String day;
        private Integer regCount; // 新注册用户数量
        private Integer memberCount; // 总注册用户数量
        private Integer orderCount; // 总订单数量
        private BigDecimal gmv;  //商品销售总额
    }

}
