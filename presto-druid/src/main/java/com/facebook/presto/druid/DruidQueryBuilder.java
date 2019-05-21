package com.facebook.presto.druid;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DruidQueryBuilder {
  private final List<DruidColumnHandle> columnHandles;
  private final static String url = "http://122.112.219.154:8082/druid/v2/sql";

//  public DruidQueryBuilder(List<DruidColumnHandle> columnHandles, DruidConfig config, DruidSplit split) {
  public DruidQueryBuilder(List<DruidColumnHandle> columnHandles) {
    requireNonNull(columnHandles, "columnHandles is null");
//    requireNonNull(config, "config is null");
//    requireNonNull(split, "split is null");

    this.columnHandles = columnHandles;
  }

  public List<Map<String, String>> query(String sql) {
    Map<String, String> param = new HashMap<>();
    param.put("query", sql);
    String result = HttpUtil.sendRequest(url, "POST", param);
    return parseQueryResult(result);
  }


  private List<Map<String, String>> parseQueryResult(String rs) {
    Type mapType = new TypeToken<List<Map<String, String>>>(){}.getType();
    List<Map<String, String>> rsList = new Gson().fromJson(rs, mapType);
    return rsList;
  }

}
