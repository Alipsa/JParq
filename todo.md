
- Alias support (e.g select model as type from mtcars)
- expose an INFORMATION_SCHEMA with tables/columns via in-memory result sets
- slim deps with shading/exclusions to get a leaner jar.
- GROUP BY support
- HAVING support
- Aggregation Functions support (min, max, count, avg, sum)
- CAST support
- EXISTS support
- CASE support
- ANY support e.g. SELECT ProductName
  FROM Products
  WHERE ProductID = ANY
  (SELECT ProductID
  FROM OrderDetails
  WHERE Quantity = 10);
- ALL support e.g. SELECT ProductName
  FROM Products
  WHERE ProductID = ALL
  (SELECT ProductID
  FROM OrderDetails
  WHERE Quantity = 10);
- COMMENTS support
- string functions support
- numeric function support
- date functions support
- JOIN support (join, inner join, left join, right join, full join)
- UNION support
