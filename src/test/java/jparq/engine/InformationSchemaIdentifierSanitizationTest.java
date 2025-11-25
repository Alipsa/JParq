package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.IdentifierUtil;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.meta.InformationSchemaColumns;
import se.alipsa.jparq.meta.InformationSchemaTables;

/** Tests sanitization of information schema identifiers during parsing. */
class InformationSchemaIdentifierSanitizationTest {

  @Test
  void parseQuotedInformationSchemaTablesUsesSanitizedSchemaName() {
    SqlParser.Select select = SqlParser.parseSelect("SELECT * FROM \"INFORMATION_SCHEMA\".\"TABLES\"");

    SqlParser.TableReference reference = select.tableReferences().get(0);

    assertEquals(InformationSchemaTables.TABLE_IDENTIFIER, reference.tableName());
    assertEquals(IdentifierUtil.sanitizeIdentifier("\"INFORMATION_SCHEMA\""), reference.schemaName());
  }

  @Test
  void parseQuotedInformationSchemaColumnsUsesSanitizedSchemaName() {
    SqlParser.Select select = SqlParser.parseSelect("SELECT * FROM \"INFORMATION_SCHEMA\".\"COLUMNS\"");

    SqlParser.TableReference reference = select.tableReferences().get(0);

    assertEquals(InformationSchemaColumns.TABLE_IDENTIFIER, reference.tableName());
    assertEquals(IdentifierUtil.sanitizeIdentifier("\"INFORMATION_SCHEMA\""), reference.schemaName());
  }
}
