package com.facebook.presto.druid;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.common.reflect.TypeToken;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.lang.reflect.Type;


public class HttpUtil {
  private static final int CONN_TIMEOUT = 60000;
  private static final int READ_TIMEOUT = 60000;


  public static String sendRequest(String url, String method, Map params){
    HttpURLConnection conn = null;
    BufferedReader reader = null;
    String rs = null;
    try {
      StringBuffer sb = new StringBuffer();
      if (method == null || method.equals("GET")) {
        url = url + "?" + handleParam(method, params);
      }
      URL finalUrl = new URL(url);
      conn = (HttpURLConnection) finalUrl.openConnection();
      if (method == null || method.equals("GET")) {
        conn.setRequestMethod("GET");
      } else if (method.equals("POST")) {
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
      }
      conn.setUseCaches(false);
      conn.setConnectTimeout(CONN_TIMEOUT);
      conn.setReadTimeout(READ_TIMEOUT);
      conn.connect();
      if (params != null && method.equals("POST")) {
        try {
          DataOutputStream out = new DataOutputStream(conn.getOutputStream());
          out.writeBytes(handleParam(method, params));
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      InputStream is = conn.getInputStream();
      reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
      String strRead = null;
      while((strRead = reader.readLine()) != null) {
        sb.append(strRead);
      }
      rs = sb.toString();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (conn != null) {
        conn.disconnect();
      }
    }
    return rs;
  }

  public static String handleParam(String method, Map<String, Object> data) {
    if (method.equals("GET")) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry i : data.entrySet()) {
        sb.append(i.getKey()).append("=").append(i.getValue()).append("&");
      }
      return sb.toString();
    } else if (method.equals("POST")) {
      return mapToJson(data);
    } else {
      return "";
    }
  }

  public static String mapToJson(Map<String, Object> source) {
    GsonBuilder builder = new GsonBuilder();
    Gson gson = builder.create();
    return gson.toJson(source);
  }

  public static Map<String, List<String>> jsonToMap(String source) {
    Type mapType = new TypeToken<Map<String, List<String>>>(){}.getType();
    Map<String, List<String>> result = new Gson().fromJson(source, mapType);
    return result;
  }

}
