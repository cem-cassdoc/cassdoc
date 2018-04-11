package cassdoc.springmvc.controller

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.util.regex.Pattern

import javax.servlet.http.HttpServletRequest

import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext

@Slf4j
@RestController
@CompileStatic
class LogAdminController {

	static List<String> logLevels = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'all', 'off']

	@RequestMapping(value = '/logadmin', method = RequestMethod.GET)
	List<LoggerInfo> logLevelAdmin(
			HttpServletRequest req,
			@RequestParam(value = 'op', required = false) String targetOperation,
			@RequestParam(value = 'log', required = false) String targetLogger,
			@RequestParam(value = 'lvl', required = false) String targetLogLevel,
			@RequestParam(value = 'inclhas', required = false) String hasFilter,
			@RequestParam(value = 'inclsw', required = false) String startsWithFilter,
			@RequestParam(value = 'inclrx', required = false) String regexFilter,
			@RequestParam(value = 'excl', required = false) List<String> excludeFilters
	) {

		Map<String, Logger> loggersMap = calculateLoggersMap(hasFilter, startsWithFilter, regexFilter, excludeFilters)

		List<String> keys = loggersMap.keySet().toList().sort { Object a, Object b ->
			a.toString().compareToIgnoreCase b.toString()
		}
		String baseUrl = "${req.scheme}://${req.serverName}:${req.serverPort}${req.contextPath}${req.requestURI}"

		List<LoggerInfo> loggerInfos = []
		keys.each { String key ->
			Logger logger = (Logger) loggersMap.get(key)
			if ('c'.equalsIgnoreCase(targetOperation) && targetLogger == logger.name) {
				loggersMap[targetLogger]?.setLevel(Level.toLevel(targetLogLevel))
			}
			LoggerInfo loggerInfo = new LoggerInfo(name: logger.name, level: logger.effectiveLevel.toString())
			loggerInfo.links = logLevels.collect {
				String link = (
						baseUrl +  "?op=c&lvl=$it&log=${loggerInfo.name}" +
						"${hasFilter ? '&inclhas=' + hasFilter : ''}" +
						"${regexFilter ? '&inclrx=' + regexFilter : ''}" +
						"${startsWithFilter ? '&inclsw=' + startsWithFilter : ''}")
				if (excludeFilters) {
					StringBuilder excls = new StringBuilder()
					excludeFilters.each {
						excls.append('&excl=').append(it)
					}
					link += excls.toString()
				}
				link
			}
			loggerInfos.add(loggerInfo)
		}
		return loggerInfos
	}

	Map calculateLoggersMap(
			String hasFilter,
			String startsWithFilter,
			String regexFilter,
			List<String> excludeFilters) {
		Pattern p = null
		if (regexFilter != null && regexFilter.trim() != '') {
			p = Pattern.compile(regexFilter)
		}
		Map loggersMap = [:]
		Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)
		loggersMap[rootLogger.name] = rootLogger

		((LoggerContext) LoggerFactory.ILoggerFactory).loggerList.each { Logger logger ->
			boolean exclFiltered = false
			Boolean inclFiltered = null
			if (excludeFilters) {
				if (excludeFilters.find { logger.name.startsWith(it) }) {
					exclFiltered = true
				}
			}

			if (hasFilter != null && hasFilter.trim() != '') {
				if (logger.name.toUpperCase().indexOf(hasFilter.toUpperCase()) >= 0) {
					// containsFilter overrides exclusion filters...
					inclFiltered = false
				} else {
					inclFiltered = true
				}
			}
			if (startsWithFilter != null && startsWithFilter.trim() != '') {
				if (logger.name.startsWith(startsWithFilter)) {
					inclFiltered = false
				} else {
					inclFiltered = true
				}
			}
			if (p != null) {
				if (p.matcher(logger.name).matches()) {
					inclFiltered = false
				} else {
					inclFiltered = true
				}
			}

			if (inclFiltered == null && !exclFiltered) {
				loggersMap[logger.name] = logger
			} else {
				// include can override exclude filters
				if (!inclFiltered) {
					loggersMap[logger.name] = logger
				}
			}
		}

		return loggersMap
	}

}

class LoggerInfo {
	String name
	String level
	List<String> links
}

