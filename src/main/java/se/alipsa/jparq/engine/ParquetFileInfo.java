package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

/**
 * Footer information about a Parquet file, independent of JDBC metadata.
 * 
 * @param rowGroups
 *          the row groups in file order
 */
public record ParquetFileInfo(List<RowGroup> rowGroups) {
  /**
   * Copies the row groups to keep footer information immutable.
   * 
   * @param rowGroups
   *          the row groups in file order
   */
  public ParquetFileInfo {
    rowGroups = List.copyOf(rowGroups);
  }

  /**
   * Describes row counts and index availability for every column chunk in a row
   * group.
   * 
   * @param rowCount
   *          the number of rows
   * @param columnIndex
   *          whether every column chunk has a column index
   * @param anyIndex
   *          whether any column chunk has either index
   * @param offsetIndex
   *          whether every column chunk has an offset index
   */
  public record RowGroup(long rowCount, boolean columnIndex, boolean offsetIndex, boolean anyIndex) {
    /**
     * Preserves the original constructor for callers with complete-index flags.
     * 
     * @param rowCount
     *          row count
     * @param columnIndex
     *          all chunks have column indexes
     * @param offsetIndex
     *          all chunks have offset indexes
     */
    public RowGroup(long rowCount, boolean columnIndex, boolean offsetIndex) {
      this(rowCount, columnIndex, offsetIndex, columnIndex || offsetIndex);
    }

  }

  /**
   * Reads row counts and index references without reading data pages.
   * 
   * @param path
   *          the Parquet file
   * @param configuration
   *          the reader configuration
   * @return immutable footer information
   * @throws IOException
   *           if the footer cannot be read
   */
  public static ParquetFileInfo read(Path path, Configuration configuration) throws IOException {
    List<RowGroup> groups = new ArrayList<>();
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, configuration))) {
      for (var group : reader.getFooter().getBlocks()) {
        boolean columnIndex = !group.getColumns().isEmpty();
        boolean offsetIndex = columnIndex;
        boolean anyIndex = false;
        for (var column : group.getColumns()) {
          columnIndex &= column.getColumnIndexReference() != null;
          offsetIndex &= column.getOffsetIndexReference() != null;
          anyIndex |= column.getColumnIndexReference() != null || column.getOffsetIndexReference() != null;
        }
        groups.add(new RowGroup(group.getRowCount(), columnIndex, offsetIndex, anyIndex));
      }
    }
    return new ParquetFileInfo(groups);
  }

  /**
   * Totals the footer row counts.
   * 
   * @return the number of rows before filtering
   */
  public long rowCount() {
    return rowGroups.stream().mapToLong(RowGroup::rowCount).sum();
  }

  /**
   * Summarizes page indexes across all chunks, including partially indexed files.
   * 
   * @return present, absent, or partial
   */
  public String pageIndexStatus() {
    if (pageIndexPresent()) {
      return "present";
    }
    return rowGroups.stream().anyMatch(RowGroup::anyIndex) ? "partial" : "absent";
  }

  /**
   * Tests whether all column chunks have both page indexes.
   * 
   * @return whether the file is fully indexed
   */
  public boolean pageIndexPresent() {
    return !rowGroups.isEmpty() && rowGroups.stream().allMatch(group -> group.columnIndex() && group.offsetIndex());
  }
}
