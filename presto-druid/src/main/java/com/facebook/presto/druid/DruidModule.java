package com.facebook.presto.druid;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

public class DruidModule implements Module{

  private final String connectorId;
  private final TypeManager typeManager;

  public DruidModule(String connectorId, TypeManager typeManager)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null");
    this.typeManager = requireNonNull(typeManager, "typeManager is null");
  }
  @Override
  public void configure(Binder binder) {
    binder.bind(TypeManager.class).toInstance(typeManager);

    binder.bind(DruidConnector.class).in(Scopes.SINGLETON);
    binder.bind(DruidConnectorId.class).toInstance(new DruidConnectorId(connectorId));
    binder.bind(DruidMetadata.class).in(Scopes.SINGLETON);
    binder.bind(DruidClient.class).in(Scopes.SINGLETON);
    binder.bind(DruidSplitManager.class).in(Scopes.SINGLETON);
    binder.bind(DruidRecordSetProvider.class).in(Scopes.SINGLETON);
    configBinder(binder).bindConfig(DruidConfig.class);

    jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
    jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(DruidTable.class));
  }

  public static final class TypeDeserializer extends FromStringDeserializer<Type>
  {
    private final TypeManager typeManager;

    @Inject
    public TypeDeserializer(TypeManager typeManager)
    {
      super(Type.class);
      this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected Type _deserialize(String value, DeserializationContext context)
    {
      return typeManager.getType(parseTypeSignature(value));
    }
  }
}
