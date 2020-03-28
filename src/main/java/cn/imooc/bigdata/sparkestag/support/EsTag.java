package cn.imooc.bigdata.sparkestag.support;

import lombok.Data;

/**
 * EsTag数据定义类
 */
@Data
public class EsTag {
    private String name;
    private String value;
    private String type;
}
