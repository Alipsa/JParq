- Read the README.md for basic project understanding.
- Always create tests to verify any new functionality. Test are in the jparq package (library tests such as test for jsqlparser functionality are outside jparq however). Since the main code is in the se.alipsa.jparq package some methods must be public to allow testing, it is preferred to do this over resorting to reflection.
- Remember to also update javadocs (all classes and methods must have a description, all params must be listed and return and throws specified when appropriate) where needed.
- Use spelling rules for American English
- Avoid using deprecated classes or methods if possible, prefer modern alternatives.
- Adhere to the coding standard defined in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml and also pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself).
- All tests must pass after the implementation to ensure that there is no regression. If the `mvn verify`  command fails you shall try running it without pmd and checkstyle to see if the failure is due to those tools or actual test failures. i.e: 
  `mvn -Dspotless.check.skip=true -Dpmd.skip=true -Dcheckstyle.skip=true verify` in that case you must make sure the code complies with the rules in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml without relying on the skip flags to confirm.
- No checkstyle, PMD or Spotless violations can be present after the implementation.
