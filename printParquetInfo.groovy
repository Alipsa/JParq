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
import org.apache.parquet.schema.PrimitiveType

import java.io.File
import java.io.IOException

class ParquetMetadataPrinter {

  def printMetadata(File parquetFile) {
    def filePath = new Path(parquetFile.getPath())
    def conf = new Configuration()
    def totalRecords = 0L

    println "--- Analyzing Parquet File: ${parquetFile.getName()} ---"

    try {
      // 1. Setup Hadoop configuration and read options
      ParquetReadOptions options = HadoopReadOptions.builder(conf).build()
      InputFile inputFile = HadoopInputFile.fromPath(filePath, conf)

      // 2. Open the ParquetFileReader to access the file footer
      ParquetFileReader reader = ParquetFileReader.open(inputFile, options)
      try {
        def readFooter = reader.getFooter()
        def fileMetaData = readFooter.getFileMetaData()

        if (!fileMetaData) {
          println "❌ Error: File metadata is missing."
          return
        }

        // --- 3. Extract and Print Schema (Column Names and Types) ---
        def schema = fileMetaData.getSchema()
        println "\n[SCHEMA DETAILS]"
        println "Total Columns: ${schema.getFieldCount()}"

        println "Columns:"
        schema.getFields().each { field ->
          def typeString = "GROUP" // Default for nested/complex types
          if (field instanceof PrimitiveType) {
            typeString = field.getPrimitiveTypeName().toString()
          }

          // Print column name and Parquet Type (e.g., INT64, BINARY)
          println "  - ${field.getName()}: ${typeString}"
        }

        // --- 4. Extract and Print Row Count ---
        readFooter.getBlocks().each { BlockMetaData block ->
          totalRecords += block.getRowCount()
        }

        println "\n[FILE STATISTICS]"
        println "Total Rows: ${totalRecords}"

        // --- UPDATED: Dynamic File Size Formatting ---
        def totalByteSize = readFooter.getBlocks().collect { it.getTotalByteSize() }.sum()
        def sizeString

        if (totalByteSize < 1024) {
          // Less than 1 KB -> use bytes
          sizeString = "${totalByteSize} Bytes"
        } else if (totalByteSize < 1024 * 1024) {
          // Less than 1 MB -> use KB
          sizeString = String.format('%.2f KB', totalByteSize / 1024.0)
        } else {
          // 1 MB or more -> use MB
          sizeString = String.format('%.2f MB', totalByteSize / 1024.0 / 1024.0)
        }

        println "File Size (data only): ${sizeString}"

        println "\n--- Analysis Complete ---"

      } finally {
        reader.close() // Ensure the reader is closed
      }

    } catch (IOException e) {
      println "❌ FAILED: Could not read Parquet file metadata."
      println "Error: ${e.getMessage()}"
    } catch (Exception e) {
      println "❌ An unexpected error occurred: ${e.getMessage()}"
    }
  }
}

// Groovy script execution block
if (this.args.length == 0) {
  println "Usage: groovy ParquetMetadataPrinter.groovy <path_to_parquet_file>"
  return
}

def file = new File(this.args[0])

if (!file.exists() || !file.isFile()) {
  println "Provided path is not a valid file: ${this.args[0]}"
  return
}

def printer = new ParquetMetadataPrinter()
printer.printMetadata(file)
