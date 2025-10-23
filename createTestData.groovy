@Grab("se.alipsa.matrix:matrix-core:3.5.0")
@Grab("se.alipsa.matrix:matrix-datasets:2.1.1")
@Grab("se.alipsa.matrix:matrix-parquet:0.3.0")

import se.alipsa.matrix.datasets.Dataset

import se.alipsa.matrix.parquet.MatrixParquetWriter

MatrixParquetWriter.write(Dataset.mtcars(), new File("src/test/resources/mtcars.parquet"), false)
println("done")