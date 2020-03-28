package cn.imooc.bigdata.sparkestag.web;

import cn.imooc.bigdata.sparkestag.etl.es.EsMappingEtl;
import cn.imooc.bigdata.sparkestag.service.EsQueryService;
import cn.imooc.bigdata.sparkestag.support.EsTag;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.glassfish.jersey.message.internal.StringBuilderUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.Servlet;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

@Controller
public class EsQueryController {
    // 和ES进行交互的service
    @Autowired
    EsQueryService service;

    @RequestMapping("/gen")
    public void genAndDown(@RequestBody String data, HttpServletResponse response) {
        JSONObject object = JSON.parseObject(data);
        JSONArray selectedTags = object.getJSONArray("selectedTags"); // 前端点选的标签
        List<EsTag> list = selectedTags.toJavaList(EsTag.class);
        List<EsMappingEtl.MemberTag> tags = service.buildQuery(list);
        String content = toContent(tags);
        // 返回给前端
        String fileName = "member.txt";
        response.setContentType("text/plain");

        try {
            response.setHeader("Content-Disposition","attachment; filename=" + URLEncoder.encode(fileName, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        // 定义io流
        ServletOutputStream sos = null;
        BufferedOutputStream bos = null;
        try {
            sos = response.getOutputStream();
            bos = new BufferedOutputStream(sos);
            bos.write(content.getBytes("UTF-8"));
            bos.flush();
            bos.close();
            sos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String toContent(List<EsMappingEtl.MemberTag> tags) {
        StringBuilder sb = new StringBuilder();
        // 返回值
        for (EsMappingEtl.MemberTag tag : tags) {
            sb.append("["+tag.getMemberId()+","+tag.getPhone()+"]");
        }
        return sb.toString();
    }

}
