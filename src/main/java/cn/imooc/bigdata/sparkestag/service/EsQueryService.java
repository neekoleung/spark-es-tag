package cn.imooc.bigdata.sparkestag.service;

import cn.imooc.bigdata.sparkestag.etl.es.EsMappingEtl;
import cn.imooc.bigdata.sparkestag.support.EsTag;
import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Service
public class EsQueryService {

    @Resource(name = "highLevelClient")
    RestHighLevelClient highLevelClient;

    public List<EsMappingEtl.MemberTag> buildQuery(List<EsTag> tags) {
        SearchRequest request = new SearchRequest();
        request.indices("tag");
        request.types("_doc");
        // 要生成的字段
        String[] includes = {"memberId","phone"};

        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        builder.query(boolQueryBuilder);
        builder.from(0);
        builder.size(1000); // 分页：0-1000
        builder.fetchSource(includes,null); // 定义返回的字段，防止网络传输过程中数据量过大造成的性能下降
        List<QueryBuilder> should = boolQueryBuilder.should();
        List<QueryBuilder> mustNot = boolQueryBuilder.mustNot();
        List<QueryBuilder> must = boolQueryBuilder.must();

        for (EsTag tag : tags) {
            String name = tag.getName();
            String value = tag.getValue();
            String type = tag.getType();
            // 三种type
            if (type.equals("match")) {
                should.add(QueryBuilders.matchQuery(name,value));
            }

            if (type.equals("notMatch")) {
                mustNot.add(QueryBuilders.matchQuery(name,value));
            }

            if (type.equals("rangeBoth")) {
                String[] split = value.split("-");
                String v1 = split[0];
                String v2 = split[1];
                should.add(QueryBuilders.rangeQuery(name).lte(v2).gte(v1));
            }

            if (type.equals("rangeGte")) {
                should.add(QueryBuilders.rangeQuery(name).gte(value));
            }

            if (type.equals("rangeLte")) {
                should.add(QueryBuilders.rangeQuery(name).lte(value));
            }

            if (type.equals("exists")) {
                should.add(QueryBuilders.existsQuery(name));
            }
        }

        // 输出生成的DSL语句
        String s = boolQueryBuilder.toString();
        System.out.println("DSL:");
        System.out.println(s);

        // 把request和builder联系起来
        request.source(builder);
        RequestOptions options = RequestOptions.DEFAULT;

        SearchResponse search = null;
        List<EsMappingEtl.MemberTag> memberTags = new ArrayList<>();
        try {
            search = highLevelClient.search(request, options);
            SearchHits hits = search.getHits();
            Iterator<SearchHit> iterator = hits.iterator();
            while (iterator.hasNext()) {
                SearchHit next = iterator.next();
                String sourceAsString = next.getSourceAsString();
                EsMappingEtl.MemberTag memberTag = JSON.parseObject(sourceAsString, EsMappingEtl.MemberTag.class);
                memberTags.add(memberTag);
            }
            return memberTags;
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 存在异常，返回null
        return null;
    }
}
