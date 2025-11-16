- Read the README.md for basic project understanding.
- Always create tests to verify any new functionality
- Remember to also update javadocs (all classes and methods must have a description, all params must be listed and return and throws specified when appropriate) where needed.
- Use spelling rules for American English
- Avoid using deprecated classes or methods if possible, prefer modern alternatives.
- Adhere to the coding standard defined in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml and also pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself).
- All tests must pass after the implementation using `mvn -Dspotless.check.skip=true verify` to ensure that there is no regression. If that command fails you shall try running it without pmd and checkstyle to see if the failure is due to those tools or actual test failures. i.e: 
  `mvn -Dspotless.check.skip=true -Dpmd.skip=true -Dcheckstyle.skip=true verify` in that case you must make sure the code complies with the rules in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml without relying on the skip flags to confirm.
- No checkstyle, PMD or Spotless violations can be present after the implementation.
