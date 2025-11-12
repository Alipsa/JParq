/*
Note, this script MUST be run on java 21 or lesser (8-21 is fine) due to Hadoop constraints.
 */
@GrabConfig(systemClassLoader=true)
@Grab('org.slf4j:slf4j-simple:2.0.17')
@Grab('se.alipsa.matrix:matrix-core:3.5.0')
@Grab('se.alipsa.matrix:matrix-parquet:0.3.0')
@Grab('se.alipsa.matrix:matrix-sql:2.2.0')
@Grab('com.h2database:h2:2.4.240')

import groovy.ant.AntBuilder
import groovy.transform.SourceURI
import groovy.transform.Field
import se.alipsa.matrix.core.Matrix
import se.alipsa.matrix.core.ListConverter
import se.alipsa.matrix.parquet.MatrixParquetWriter
import se.alipsa.matrix.sql.MatrixSql
import se.alipsa.matrix.sql.MatrixSqlFactory

import java.time.LocalDate

@SourceURI
@Field
URI sourceUri

@Field
File scriptDir = new File(sourceUri).parentFile

employees = Matrix.builder('employees').data(
  id: [1,2,3,4,5],
  first_name: ['Per', 'Karin', 'Tage', 'Arne', 'Sixten'],
  last_name:  ['Andersson','Pettersson','Lundström','Larsson','Svensson']
).types(int, String, String)
.build()
MatrixParquetWriter.write(employees, new File(scriptDir, 'resources/acme/employees.parquet'))
println "exported employees"

departments = Matrix.builder('departments').data(
  id: [1,2,3],  
  department: ['IT', 'HR', 'Sales']
).types(int, String).build()
MatrixParquetWriter.write(employees, new File(scriptDir,'resources/acme/departments.parquet'))
println "exported departments"

employeeDepartments = Matrix.builder('employee_department').data(
  id: [1,2,3,4,5],
  department: [1,1,2,3,3],
  employee: [1,2,3,4,5]
).types(int, int, int).build()
MatrixParquetWriter.write(employeeDepartments, new File(scriptDir,'resources/acme/employee_department.parquet'))
println("exported employee_department")

salary = Matrix.builder('salary').data(
  id: [1,2,3,4,5,6,7,8],
  employee: [1,2,3,4,5,1,3,1],
  salary: [150000,180000,130000,195000,230000,160000,140000,165000],
  change_date: ListConverter.toLocalDates('2020-03-01','2020-03-01','2021-01-01','2020-10-01','2020-12-15','2021-01-01','2021-12-01','2021-08-01')
).types(int, int, BigDecimal, LocalDate).build()
MatrixParquetWriter.write(salary, new File(scriptDir,'resources/acme/salary.parquet'))
println("exported salary")


if (false) {
  println "creating nested products data"

// Data definition
  def productIds = [101, 102, 103]
  def productNames = ['Laptop Pro', 'Mouse Pad', 'Monitor Ultra']
  def productTags = [
      ['Electronics', 'High-End'],             // tags: Array of String
      ['Accessories', 'Low-Cost', 'Desk'],     // tags: Array of String
      ['Electronics', 'Display', '4K']         // tags: Array of String
  ]
  def productDetails = [
      [manufacturer: 'Alpha', warranty_years: 3], // details: Single Record/ROW
      [manufacturer: 'Beta', warranty_years: 1],
      [manufacturer: 'Gamma', warranty_years: 2]
  ]
  def productReviews = [
      [[rating: 5, user: 'A'], [rating: 4, user: 'B']], // reviews: Array of Records/ROW ARRAY
      [[rating: 2, user: 'C']],
      [[rating: 5, user: 'D'], [rating: 5, user: 'E'], [rating: 4, user: 'F']]
  ]

  productsNested = Matrix.builder('products_nested').data(
      id: productIds,
      name: productNames,
      tags: productTags,
      details: productDetails,
      reviews: productReviews
  ).build()
  MatrixParquetWriter.write(productsNested, new File(scriptDir, 'resources/acme/products_nested.parquet'))
  println("exported products_nested with array and record types")
}
println "creating h2 database"

String h2FileName = 'acmeh2'
File dbFile = new File(scriptDir, "resources/$h2FileName")
def ant = new AntBuilder()
ant.delete {
  fileset(dir: 'resources') {
    include(name: "$h2FileName*")
  }
}
try(MatrixSql matrixSql = MatrixSqlFactory.createH2(dbFile, 'sa', '', 'DATABASE_TO_UPPER=FALSE;CASE_INSENSITIVE_IDENTIFIERS=TRUE')) {
  matrixSql.create(employees)
  matrixSql.create(departments)
  matrixSql.create(employeeDepartments)
  matrixSql.create(salary)
  /*
  matrixSql.update("DROP TABLE IF EXISTS products_nested;")
  matrixSql.update("""
      CREATE TABLE products_nested (
          id INT PRIMARY KEY,
          name VARCHAR(255),
          tags ARRAY, 
          details ROW(manufacturer VARCHAR(50), warranty_years INT),
          reviews ARRAY 
      );
  """)
  productIds.eachWithIndex { id, i ->
    def tagsSql = productTags[i].collect { "'$it'" }.join(', ')
    def details = productDetails[i]
    def reviewsSql = productReviews[i].collect { "( ${it.rating}, '${it.user}' )" }.join(', ')

    String sql = """
        INSERT INTO products_nested (id, name, tags, details, reviews)
        VALUES (
            ${id},
            '${productNames[i]}',
            ARRAY[${tagsSql}], 
            ROW('${details.manufacturer}', ${details.warranty_years}), 
            ARRAY[ROW(rating INT, user VARCHAR) : ( ${reviewsSql} )]
        );
    """
    println sql
    matrixSql.update(sql)
  }
   */
  println "h2 database created at ${dbFile.absolutePath}"
}
println "Done!"