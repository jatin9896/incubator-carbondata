package org.apache.carbondata.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCarbondataTableLayoutHandle {

  private static SchemaTableName schemaTableName = new SchemaTableName("schema","table");
  private static CarbondataTableHandle table = new CarbondataTableHandle("connectorId",schemaTableName);
  private static TupleDomain<ColumnHandle> constraint = TupleDomain.all();

  CarbondataTableLayoutHandle carbondataTableLayoutHandle = new CarbondataTableLayoutHandle(table,constraint);

  @Test
  public void getTableTest() {
    assert(carbondataTableLayoutHandle.getTable() != null);
  }

  @Test
  public void getConstraintTest() {
    assert(carbondataTableLayoutHandle.getConstraint() != null);
  }

  @Test
  public void toStringTest() {
    assertEquals(true,carbondataTableLayoutHandle.toString().contains("table"));
  }

  @Test
  public void hashCodeTest() {
    assertEquals(-1917799399,carbondataTableLayoutHandle.hashCode());
  }

  @Test
  public void equalsTestObjectNull() {
    assertEquals(false,carbondataTableLayoutHandle.equals(new Object()));
  }

  @Test
  public void equalsTestObject() {
    assertTrue(carbondataTableLayoutHandle.equals(new CarbondataTableLayoutHandle(table,constraint)));
  }

  @Test
  public void equalsTestThisObject() {
    assertTrue(carbondataTableLayoutHandle.equals(carbondataTableLayoutHandle));
  }

}