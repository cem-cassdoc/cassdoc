package cassdoc.springmvc.config

import cwdrg.spring.annotation.RequestParameterJSONArgumentResolver
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.method.support.HandlerMethodArgumentResolver
import org.springframework.web.servlet.config.annotation.EnableWebMvc
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter

@Configuration
@EnableWebMvc
class WebConfig extends WebMvcConfigurerAdapter {

    @Override
    void addArgumentResolvers(List<HandlerMethodArgumentResolver> argumentResolvers) {
        // equivalent to <mvc:argument-resolvers>
        argumentResolvers.add(resolver())
    }


    //@Override
    //void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
    //    // equivalent to <mvc:message-converters>
    //}

    @Bean
    RequestParameterJSONArgumentResolver resolver() { new RequestParameterJSONArgumentResolver() }

}