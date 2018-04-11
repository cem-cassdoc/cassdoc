package cassdoc.testapi

import spock.lang.Specification

class GetDataSpec extends Specification {
    // sometimes this is wonky in various IDEs (ahem, IntelliJ) so this test checks it works
    // if Intellij, need to do Preferences -> Compiler -> Resource Patterns config (add yml, yaml, and json)
    // note that resource files need to be indicated in the gradle.build too
    void "test if get resource from classpath works"() {
        when:
        String doc = this.class.classLoader.getResourceAsStream('cassdoc/testdata/SimpleDoc.json')?.getText()
        then:
        doc != null
    }
}
