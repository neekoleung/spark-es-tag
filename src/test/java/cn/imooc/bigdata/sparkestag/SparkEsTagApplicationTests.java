package cn.imooc.bigdata.sparkestag;

import cn.imooc.bigdata.sparkestag.etl.es.EsMappingEtl;
import cn.imooc.bigdata.sparkestag.service.EsQueryService;
import cn.imooc.bigdata.sparkestag.support.EsTag;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * service测试用例
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class SparkEsTagApplicationTests {
    @Autowired
    EsQueryService service;

    @Test
    public void contextLoads() {
        List<EsTag> list = new ArrayList<>();
        EsTag tag = new EsTag();
        tag.setType("match");
        tag.setName("sex");
        tag.setValue("1");
        list.add(tag);
        List<EsMappingEtl.MemberTag> tags = service.buildQuery(list);
    }

}
