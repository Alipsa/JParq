
@GrabConfig(systemClassLoader=true)
@Grab("se.alipsa:jparq:0.11.0")
@Grab('tools.jackson.core:jackson-databind:3.0.2')
@Grab('se.alipsa.matrix:matrix-core:3.5.0')
import se.alipsa.jparq.JParqSql
import se.alipsa.matrix.core.Matrix

String userDir = System.getProperty("user.dir")
jparqSql = new JParqSql("jdbc:jparq:${userDir}/src/test/resources/acme")

 String sql = """
  select e.*, s.salary from employees e join salary s on e.id = s.employee
  WHERE
      s.change_date = (
          SELECT
              MAX(s2.change_date)
          FROM
              salary s2
          WHERE
              s2.employee = s.employee
      );
  """
List<String> rows = []
jparqSql.query(sql) { rs ->
  List header = []
  for (int i = 1; i <= rs.metaData.columnCount; i++) {
    header << rs.metaData.getColumnName(i) + ' (' + rs.metaData.getColumnTypeName(i) + ')'
  }
  rows << String.join(' ', header)
  while (rs.next()) {
    String id = rs.getString(1)
    String firstName = rs.getString(2)
    String lastName = rs.getString(3)
    String salary = rs.getString(4)
    rows << "${id.padLeft(header[0].length())} ${firstName.padRight(header[1].length())} ${lastName.padRight(header[2].length())} ${salary.padLeft(header[3].length())}"
  }
}

// Creating a Matrix from the resultSet
jparqSql.query(sql) { rs -> 
  matrix = Matrix.builder().data(rs).build()
  print "Column types: "
  matrix.types().each {
    print "$it.simpleName, "
  }
  println ''
  println matrix.content()
}

println String.join('\n', rows)