Please add support for XXX as described in the SQL:1999 and SQL:2003 standards

# Important!
- Create test to verify the functionality in a test class called jparq.XXXTest.java in the src/test/java/se/alipsa/jparq directory.
- Remember to also update javadocs (all classes and public methods must have a description, all params must be listed and return and throws specified when appropriate) where needed.
- All tests must pass after the implementation using `mvn -Dspotless.check.skip=true test` to ensure that there is no regression.
- Adhere to the coding standard defined in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml and also pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself).
