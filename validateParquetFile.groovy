@GrabConfig(systemClassLoader=true) // Loads SLF4J into the system classpath to suppress warnings
@Grab(group='org.apache.parquet', module='parquet-avro', version='1.16.0')
@Grab(group='org.apache.parquet', module='parquet-hadoop', version='1.16.0')
@Grab(group='org.apache.avro', module='avro', version='1.12.1')
@Grab(group='org.apache.hadoop', module='hadoop-common', version='3.4.2')
@Grab(group='org.apache.hadoop', module='hadoop-mapreduce-client-core', version='3.4.2')
@Grab(group='org.slf4j', module='slf4j-api', version='2.0.17')
@Grab(group='org.slf4j', module='slf4j-simple', version='2.0.17')

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.InputFile
import java.io.File
import java.io.IOException

// The class definition in Groovy is almost identical, just removing 'public' and
// using 'def' for Groovy-style dynamic typing.
class ParquetValidator {

  // Method to check the file validity
  def isParquetFileValid(File parquetFile) {
    def filePath = new Path(parquetFile.getPath())
    def conf = new Configuration()
    def totalRecords = 0L // Use L for long literal

    try {
      // 1. Create the Configuration-backed read options.
      //    We declare it as ParquetReadOptions (the type returned by the builder).
      ParquetReadOptions options = HadoopReadOptions.builder(conf).build()

      // 2. Create the Hadoop-specific InputFile.
      InputFile inputFile = HadoopInputFile.fromPath(filePath, conf)

      // 3. Open the ParquetFileReader
      ParquetFileReader reader = ParquetFileReader.open(inputFile, options)
      try {
        // 4. Get the full metadata (footer)
        def readFooter = reader.getFooter()

        // 5. Perform a basic check
        if (!readFooter.getFileMetaData()) {
          println "❌ Validation Failed: File metadata is missing."
          return false
        }

        // 6. Get total row count by summing records from all BlockMetaData (Row Groups)
        readFooter.getBlocks().each { BlockMetaData block ->
          totalRecords += block.getRowCount()
        }

        println "✅ The Parquet file is structurally valid."
        println "Total rows: ${totalRecords}"
        return true
      } finally {
        reader.close() // Ensure the reader is closed
      }

    } catch (IOException e) {
      // Catches errors like "file not found" or "invalid Parquet file format"
      println "❌ The Parquet file is INVALID or corrupted."
      println "Error: ${e.getMessage()}"
      return false
    } catch (Exception e) {
      // Catch any other unexpected exceptions
      println "❌ An unexpected error occurred while validating: ${e.getMessage()}"
      return false
    }
  }
}

// Groovy script execution block
if (this.args.length == 0) {
  println "Usage: groovy validateParquetFile.groovy <path_to_parquet_file>"
  return
}

def validator = new ParquetValidator()
def file = new File(this.args[0])

if (!file.exists() || !file.isFile()) {
  println "Provided path is not a valid file: ${this.args[0]}"
  return
}

if (validator.isParquetFileValid(file)) {
  println "File ${file.getName()} is valid."
} else {
  println "File ${file.getName()} is NOT valid."
}
