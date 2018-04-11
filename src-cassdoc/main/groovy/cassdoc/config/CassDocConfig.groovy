package cassdoc.config

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@EqualsAndHashCode
@CompileStatic
@ToString(includePackage = false, ignoreNulls = false, includeNames = true, includeFields = true,
        excludes = ['username', 'password']
)
@Component
@ConfigurationProperties(prefix = 'cassdoc')
class CassDocConfig {
    boolean autoCreateBaseSchema = true
    boolean autoCreateNewKeyspaces = true
    boolean autoCreateNewDocTypes = true
}
