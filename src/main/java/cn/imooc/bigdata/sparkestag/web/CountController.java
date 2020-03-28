package cn.imooc.bigdata.sparkestag.web;

import cn.imooc.bigdata.sparkestag.support.RedisUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class CountController {

    @Autowired
    RedisUtil redisUtil;

    @GetMapping("/count")
    public String count() {
        return "Now , the server's count is " +
                redisUtil.incr("count", 1L);
    }
}
