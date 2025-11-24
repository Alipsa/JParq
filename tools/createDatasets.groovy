@GrabConfig(systemClassLoader=true)
@Grab('org.slf4j:slf4j-simple:2.0.17')
@Grab('se.alipsa.matrix:matrix-core:3.5.0')
@Grab('se.alipsa.matrix:matrix-parquet:0.3.0')
@Grab('se.alipsa.matrix:matrix-datasets:2.1.1')
@Grab('se.alipsa.matrix:matrix-sql:2.2.0')
import se.alipsa.matrix.parquet.MatrixParquetWriter
import se.alipsa.matrix.datasets.Dataset

@SourceURI
@Field
URI sourceUri

@Field
File scriptDir = new File(sourceUri).parentFile

File file = new File(scriptDir, "../src/test/resources/datasets/mtcars.parquet")
file.parentFile.mkdirs()
  MatrixParquetWriter.write(
  Dataset.mtcars(),
  file
)
