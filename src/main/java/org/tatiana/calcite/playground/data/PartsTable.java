package org.tatiana.calcite.playground.data;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class PartsTable extends AbstractTable implements ScannableTable {
  protected final Object[][] rows = {
      {1, "ao", 10},
      {2, "aka", 5},
      {3, "aka", 7}
  };
  protected final RelProtoDataType protoRowType = new RelProtoDataType() {
    public RelDataType apply(final RelDataTypeFactory a0) {
      return a0.builder()
               .add("ID", SqlTypeName.INTEGER)
               .add("color", SqlTypeName.VARCHAR, 5)
               .add("units", SqlTypeName.INTEGER)
               .build();
    }
  };

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  public String toString() { return "PartsTable"; }

  public Enumerable<Object[]> scan(final DataContext root) {
    return Linq4j.asEnumerable(rows);
  }
}
