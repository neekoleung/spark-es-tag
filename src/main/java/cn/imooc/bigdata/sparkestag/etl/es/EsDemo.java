package cn.imooc.bigdata.sparkestag.etl.es;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EsDemo {
    public static void main(String[] args) {
        // 配置spark
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("es demo");
        conf.set("es.nodes","namenode");
        conf.set("es.port","9200");  // ES集群节点
        conf.set("es.index.auto.create","true"); //对新数据自动进行mapping并生成相应的index
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 插入数据到es
        List<User> list = new ArrayList<>();
        list.add(new User("Jack",18));
        list.add(new User("Eric",20));
        // 产生java的RDD
        JavaRDD<User> userJavaRDD = jsc.parallelize(list);
        // 把RDD变成ES中的数据(指定资源位置)
        JavaEsSpark.saveToEs(userJavaRDD,"/user/_doc");

        // 通过Spark进行ES的数据查询
        // 转换成Java的PairRDD
        JavaPairRDD<String, Map<String, Object>> pairRDD = JavaEsSpark.esRDD(jsc, "/user/_doc");
        Map<String, Map<String, Object>> stringMapMap = pairRDD.collectAsMap();
        System.out.println(stringMapMap);

        JavaRDD<User> rdd = pairRDD.map(new Function<Tuple2<String, Map<String, Object>>, User>() {
            @Override
            public User call(Tuple2<String, Map<String, Object>> v1) throws Exception {
                User user = new User();
                // 把key-value复制到对象当中
                BeanUtils.populate(user, v1._2());
                return user;
            }
        });

        List<User> collect = rdd.collect();
        System.out.println(collect);

        // 进行查询
        String query = "{\"query\":{\"bool\":{\"should\":[{\"match\":{\"name\":\"Eric\"}},{\"range\":{\"FIELD\":{\"gte\":30,\"lte\":40}}}]}}}";
        JavaPairRDD<String,String> pairRDD2 = JavaEsSpark.esJsonRDD(jsc,"user/_doc",query);
        Map<String, String> stringStringMap = pairRDD2.collectAsMap();
        System.out.println(stringStringMap);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class User implements Serializable {
        private String name;
        private Integer age;
    }
}
