-- Demonstrates TABLE(UNNEST(...)) per SQL:2011 Part 2, 7.2 <table primary> using a scalar array constructor.
-- The projection validates that the TABLE wrapper exposes the derived column through a strict alias.
SELECT numbers.val AS value_column
FROM TABLE(UNNEST(ARRAY[1, 2, 3])) AS numbers(val)
ORDER BY value_column;
