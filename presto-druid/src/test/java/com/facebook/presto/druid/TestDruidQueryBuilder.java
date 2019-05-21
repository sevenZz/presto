package com.facebook.presto.druid;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

public class TestDruidQueryBuilder {

  @Test
  public void testQuery() {
    List<DruidColumnHandle> columnHandles = new ArrayList<>();
    String[] columns = {"__time", "brand_code", "count", "customer_id", "item_discount", "item_list_price", "item_quantity", "item_sale_price", "item_seq", "order_no", "product_code", "shop_code", "style_code"};
    for(int i=0; i < 13; i++) {
      DruidColumnHandle handle = new DruidColumnHandle("1", columns[i], createUnboundedVarcharType(), i);
      columnHandles.add(handle);
    }

    DruidQueryBuilder builder = new DruidQueryBuilder(columnHandles);
    List<Map<String, String>> result = builder.query("SELECT * FROM pos_sale_record_analysis LIMIT 10");
    System.out.println(result);
  }
}
