package se.alipsa.jparq.util;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

public class ParquetValidator {

  public boolean isParquetFileValid(File parquetFile) {
    Path filePath = new Path(parquetFile.getPath());
    Configuration conf = new Configuration();
    long totalRecords = 0;

    try {
      ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
      InputFile inputFile = HadoopInputFile.fromPath(filePath, conf);
      try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options)) {
        ParquetMetadata readFooter = reader.getFooter();
        if (readFooter.getFileMetaData() == null) {
          System.out.println("❌ Validation Failed: File metadata is missing.");
          return false;
        }

        // 6. Get total row count by summing records from all BlockMetaData (Row Groups)
        for (BlockMetaData block : readFooter.getBlocks()) {
          totalRecords += block.getRowCount();
        }

        // The method returns without an exception, so the file is structurally valid.
        System.out.println("✅ The Parquet file is structurally valid.");
        System.out.println("Total rows: " + totalRecords);
        return true;
      }

    } catch (IOException e) {
      // Catches errors like "file not found" or "invalid Parquet file"
      System.out.println("❌ The Parquet file is INVALID or corrupted.");
      System.out.println("Error: " + e.getMessage());
      return false;
    } catch (Exception e) {
      // Catch any other unexpected exceptions during the read process
      System.out.println("❌ An unexpected error occurred while validating: " + e.getMessage());
      return false;
    }
  }

  public static void main(String[] args) {
    if (args.length == 0) {
      System.out.println("Usage: java ParquetValidator <path_to_parquet_file>");
      return;
    }

    ParquetValidator validator = new ParquetValidator();
    File file = new File(args[0]);
    if (!file.exists() || !file.isFile()) {
      System.out.println("Provided path is not a valid file: " + args[0]);
      return;
    }
    if (validator.isParquetFileValid(file)) {
      System.out.println("File " + file.getName() + " is valid.");
    } else {
      System.out.println("File " + file.getName() + " is NOT valid.");
    }
  }
}