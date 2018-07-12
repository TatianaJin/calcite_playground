package org.tatiana.calcite.playground.data;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class PartsTableFactory implements TableFactory<PartsTable>{
  public PartsTable create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
    return new PartsTable();
  }
}