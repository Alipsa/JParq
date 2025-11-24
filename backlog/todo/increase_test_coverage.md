Please enhance test coverage as follows:

1. The following helper classes in `src/main/java/se/alipsa/jparq/helper` do not have any dedicated unit tests:
*   `DateTimeExpressions`
*   `JdbcTypeMapper`
*   `JParqUtil`
*   `JsonExpressions`
*   `LiteralConverter`
*   `StringExpressions`

2. The following is a list of classes and their test coverage percentage:
    ### se.alipsa.jparq
   - JParqStatement	30 %
   - JParqDriver	46 %
   - JParqConnection	53 %
   - JParqDatabaseMetaData	69 %
   - AggregateResultSetMetaData	71 %
   - JParqResultSetMetaData	72 %
   - JParqPreparedStatement	77 %
  ### se.alipsa.jparq.engine
   - AggregateFunctions.HavingEvaluator.Operation    0 %
   - QualifiedWildcard	23 %
   - AggregateFunctions.HavingEvaluator	40 %
   - CorrelatedSubqueryRewriter	51 %
   - AvroCoercions   52 %
   - AggregateFunctions.AggregateSpec    56 %
   - ParquetFilterBuilder	62 %
   - SubqueryCorrelatedFiltersIsolator	64 %
   - AggregateFunctions.ResultColumn	67 %
   - UnnestTableBuilder	70 %	
   - AggregateFunctions	71 %	
   - SqlParser.QualifiedExpansionColumn	73 %		
   - SqlParser.TableSampleDefinition	73 %	
   - ExpressionEvaluator	62 %	
   - IdentifierUtil	76 % 	
   - ValueExpressionEvaluator	77 %	
   - JoinRecordReader.UsingMetadata	78 %	
   - ValueExpressionEvaluator.NumericType 79%
    
  ### se.alipsa.jparq.model
   - ResultSetAdapter	1 %
   - ResultSetMetaDataAdapter	6 %	
   - MetaDataResultSet.new ResultSetMetaData() {...}	27 %
   - MetaDataResultSet	60 %
   ### se.alipsa.jparq.helper
   - LiteralConverter	36 %
   - JParqUtil	45 %
   - DateTimeExpressions	46 %
   - TemporalInterval	47 %
   - JdbcTypeMapper	50 %
   - JsonExpressions	64 %
  ### se.alipsa.jparq.engine.window
  - WindowFunctions	62 %
  - WindowState	70 %
  - SumComputationState	70 %
  - AvgComputationState	74 %
  - CountComputationState	75 %
    
Since we aim for 80% test coverage, these classes needs additional tests defined.