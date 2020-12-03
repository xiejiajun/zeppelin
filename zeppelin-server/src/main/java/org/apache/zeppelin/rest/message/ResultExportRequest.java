package org.apache.zeppelin.rest.message;

import com.google.gson.Gson;
import org.apache.zeppelin.common.JsonSerializable;

/**
 * 导出结果埋点请求：自己新增的
 * @author xiejiajun
 */
public class ResultExportRequest implements JsonSerializable {

    private static final Gson gson = new Gson();

    String username;
    String paragraph;

    public ResultExportRequest(){

    }

    public String getUsername() {
        return username;
    }

    public String getParagraph() {
        return paragraph;
    }

    @Override
    public String toJson() {
        return gson.toJson(this);
    }

    public static ResultExportRequest fromJson(String json) {
        return gson.fromJson(json, ResultExportRequest.class);
    }
}
