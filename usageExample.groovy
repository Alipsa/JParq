
@GrabConfig(systemClassLoader=true)
@Grab("se.alipsa:jparq:0.10.0")
@Grab('tools.jackson.core:jackson-databind:3.0.2')
@Grab('se.alipsa.matrix:matrix-core:3.5.0')
import se.alipsa.jparq.JParqSql
import se.alipsa.matrix.core.Matrix

jparqSql = new JParqSql('jdbc:jparq:/Users/pernyf/project/JParq/src/test/resources/acme')

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
List<Integer> ids = new ArrayList<>()
salaries = new LinkedHashMap<String, Double>()
List<String> rows = []
jparqSql.query(sql) { rs ->
  String header = ''
  for (int i = 1; i <= rs.metaData.columnCount; i++) {
    header += rs.metaData.getColumnName(i) + ' (' + rs.metaData.getColumnTypeName(i) + ') '
  }
  rows << header
  while (rs.next()) {
    Integer id = rs.getInt(1);
    ids.add(id);
    String firstName = rs.getString(2);
    String lastName = rs.getString(3);
    Double salary = rs.getDouble(4);
    rows << "$id $firstName $lastName $salary"
    salaries.put(firstName + " " + lastName, salary);
  }
}
/*
[class java.lang.Object, class java.lang.Object, class java.lang.Object, class java.lang.Object]
a matrix with : 5 obs * 4 variables
e__id   first_name      last_name       salary
    1   [B@3b2317b7     [B@1142843c     165000.0
    2   [B@56c26b21     [B@3993cecb     180000.0
    3   [B@3a72517e     [B@4407f129     140000.0
    4   [B@77a14911     [B@731bdcca     195000.0
    5   [B@541c76fd     [B@76fdd5f1     230000.0
 */
jparqSql.query(sql) { rs -> 
  matrix = Matrix.builder().data(rs).build()
  matrix.println matrix.types()
  println matrix.content()
}

/*
e__id (INTEGER) first_name (BINARY) last_name (BINARY) salary (DOUBLE)
1 Per Andersson 165000.0
2 Karin Pettersson 180000.0
3 Tage Lundström 140000.0
4 Arne Larsson 195000.0
5 Sixten Svensson 230000.0
 */
println String.join('\n', rows)