package com.facebook.presto.druid;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.*;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

public class DruidClient
{
  /**
   * SchemaName -> (TableName -> TableMetadata)
   */
  private final Supplier<Map<String, Map<String, DruidTable>>> schemas;

  @Inject
  public DruidClient(DruidConfig config, JsonCodec<Map<String, List<DruidTable>>> catalogCodec)
  {
    requireNonNull(config, "config is null");
    requireNonNull(catalogCodec, "catalogCodec is null");

    schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getDatasourceMetadata(), config.getColumnMetadata()));
  }

  public Set<String> getSchemaNames() { return schemas.get().keySet(); }

  public Set<String> getTableNames(String schema)
  {
    requireNonNull(schema, "schema is null");

    Map<String, DruidTable> tables = schemas.get().get(schema);
    if (tables == null) {
      return ImmutableSet.of();
    }
    return tables.keySet();
  }

  public DruidTable getTable(String schema, String tableName)
  {
    requireNonNull(schema, "schema is null");
    requireNonNull(tableName, "tableName is null");

    Map<String, DruidTable> tables = schemas.get().get(schema);
    if (tables == null) {
      return null;
    }
    return tables.get(tableName);
  }

  private static Supplier<Map<String, Map<String, DruidTable>>> schemasSupplier(final JsonCodec<Map<String, List<DruidTable>>> catalogCodec, final URI datasourceMetadataUri, final URI columnMetadataUri)
  {
    return () -> {
      try {
        return lookupSchemas(datasourceMetadataUri, columnMetadataUri, catalogCodec);
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  private static Map<String, Map<String, DruidTable>> lookupSchemas(URI datasourceMetadataUri, URI columnMetadataUri, JsonCodec<Map<String, List<DruidTable>>> catalogCodec) throws IOException
  {
//    URL result = metadataUri.toURL();
//
//    String json = Resources.toString(result, UTF_8);
//    Map<String, List<DruidTable>> catalog = catalogCodec.fromJson(json);
//
//    return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
    String url = datasourceMetadataUri.toString();
    String result = HttpUtil.sendRequest(url, "GET", Maps.newHashMap());
    //TODO: typeof(result) is array. Make a method to transform array to Map<String, List<DruidTable>>
    Map<String, List<DruidTable>> catalogMap = catalogFromString(result, columnMetadataUri);
//    String catalogString = Joiner.on(",").withKeyValueSeparator(":").join(catalogMap);
//    Gson gson = new Gson();

//    String json = gson.toJson(catalogMap);
//    Map<String, List<DruidTable>> catalog = catalogCodec.fromJson(json);
    return ImmutableMap.copyOf(transformValues(catalogMap, resolveAndIndexTables()));
//    return null;
//    return ImmutableMap.copyOf(catalogMap);
  }

  private static Map<String, List<DruidTable>> catalogFromString(String result, URI columnMetadataUri)
  {
    List<String> datasourceList = Arrays.asList(result.substring(1, result.length() -1).split(","));
    List<DruidTable> datasources = new ArrayList<>();
    String url = columnMetadataUri.toString();
    for (String ds : datasourceList) {
      ds = ds.substring(1, ds.length()-1);
      String columnString = HttpUtil.sendRequest((url + ds), "GET", Maps.newHashMap());
      Map<String, List<String>> columnMap = HttpUtil.jsonToMap(columnString);
//      List<Map<String, String>> druidColumns = new ArrayList<Map<String, String>>();
      List<DruidColumn> druidColumns = new ArrayList<>();
      if(columnMap.get("dimensions").size() != 0){
        for (String columnName : columnMap.get("dimensions")) {
//          druidColumns.add(new HashMap<String, String>(){{put(columnName, "VARCHAR");}});
          druidColumns.add(new DruidColumn(columnName, VARCHAR));
        }
      }
      if(columnMap.get("metrics").size() != 0) {
        for (String columnName : columnMap.get("metrics")) {
//          druidColumns.add(new HashMap<String, String>() {{
//            put(columnName, "INT");
//          }});
          druidColumns.add(new DruidColumn(columnName, INTEGER));
        }
      }
//      Map<String, Object> datasource = new HashMap<>();
      DruidTable datasource = new DruidTable(ds, druidColumns);
      datasources.add(datasource);
    }

    return new HashMap<String, List<DruidTable>>(){{put("druid", datasources);}};
  }

  private static Function<List<DruidTable>, Map<String, DruidTable>> resolveAndIndexTables() //final URI metadataUri
  {
    return tables -> {
      Iterable<DruidTable> resolvedTables = transform(tables, tableUriResolver());
      return ImmutableMap.copyOf(uniqueIndex(resolvedTables, DruidTable::getName));
    };
  }

  private static Function<DruidTable, DruidTable> tableUriResolver() //final URI baseUri
  {
    return table -> {
//      List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
      return new DruidTable(table.getName(), table.getColumns());
    };
  }
}
