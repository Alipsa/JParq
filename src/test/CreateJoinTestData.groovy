/*
Note, this script MUST be run on java 21 or lesser (8-21 is fine) due to Hadoop constraints.
 */
@GrabConfig(systemClassLoader=true)
@Grab('org.slf4j:slf4j-simple:2.0.17')
@Grab('se.alipsa.matrix:matrix-core:3.5.0')
@Grab('se.alipsa.matrix:matrix-parquet:0.3.0')
@Grab('se.alipsa.matrix:matrix-sql:2.2.0')
@Grab('com.h2database:h2:2.4.240')

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
  change_date: ListConverter.toLocalDates('2020-03-01','2020-03-01','2021-01-01','2020-10-01','2020-12-15','2021-01-01','2021-01-01','2021-08-01')
).types(int, int, BigDecimal, LocalDate).build()
MatrixParquetWriter.write(salary, new File(scriptDir,'resources/acme/salary.parquet'))
println("exported salary")

println "creating h2 database"

import se.alipsa.matrix.sql.MatrixSqlFactory
import se.alipsa.matrix.sql.MatrixSql
File dbFile = new File(scriptDir, 'resources/acmeh2')
try(MatrixSql matrixSql = MatrixSqlFactory.createH2(dbFile, 'sa', '')) {
  matrixSql.create(employees)
  matrixSql.create(departments)
  matrixSql.create(employeeDepartments)
  matrixSql.create(salary)
  println "h2 database created at ${dbFile.absolutePath}"
}
println "Done!"