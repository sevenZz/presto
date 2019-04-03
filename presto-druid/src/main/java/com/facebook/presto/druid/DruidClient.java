package com.facebook.presto.druid;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
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

    schemas = Suppliers.memoize(schemaSupplier(catalogCodec, config.getMetadata()));
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

  public DruidTable getTables(String schema, String tableName)
  {
    requireNonNull(schema, "schema is null");
    requireNonNull(tableName, "tableName is null");

    Map<String, DruidTable> tables = schemas.get().get(schema);
    if (tables == null) {
      return null;
    }
    return tables.get(tableName);
  }

  private static Suppliers<Map<String, Map<String, DruidTable>>> schemaSupplier(final JsonCodec<Map<String, List<DruidTable>>> catalogCodec, final URI metadataUri)
  {
    return () -> {
      try {
        return lookupSchemas(metadataUri, catalogCodec);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  private static Map<String, Map<String, DruidTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<DruidTable>>> catalogCodec) throws IOException
  {
    URL result = metadataUri.toURL();
    String json = Resources.toString(result, UTF_8);
    Map<String, List<DruidTable>> catalog = catalogCodec.fromJson(json);

    return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
  }

  private static Function<List<DruidTable>, Map<String, DruidTable>> resolveAndIndexTables(final URI metadataUri)
  {
    return tables -> {
      Iterable<DruidTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
      return ImmutableMap.copyOf(uniqueIndex(resolvedTables, DruidTable::getName));
    };
  }

  private static Function<DruidTable, DruidTable> tableUriResolver(final URI baseUri)
  {
    return table -> {
      List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
      return new DruidTable(table.getName(), table.getColumns(), sources);
    };
  }
}
