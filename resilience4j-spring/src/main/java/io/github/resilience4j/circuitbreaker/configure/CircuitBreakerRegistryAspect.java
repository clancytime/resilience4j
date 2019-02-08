package io.github.resilience4j.circuitbreaker.configure;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerEvent;
import io.github.resilience4j.consumer.EventConsumerRegistry;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;

import java.util.Map;

@Aspect
public class CircuitBreakerRegistryAspect {
    private CircuitBreakerRegistry registry;
    private CircuitBreakerConfigurationProperties properties;
    private EventConsumerRegistry<CircuitBreakerEvent> eventConsumerRegistry;

    public CircuitBreakerRegistryAspect(CircuitBreakerRegistry registry,
                                        CircuitBreakerConfigurationProperties properties,
                                        EventConsumerRegistry<CircuitBreakerEvent> eventConsumerRegistry) {
        this.registry = registry;
        this.properties = properties;
        this.eventConsumerRegistry = eventConsumerRegistry;
    }

    @Around("execution(* io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry.circuitBreaker())")
    public Object createEventIfNew(ProceedingJoinPoint pjp) throws Throwable {
        MethodBasedEvaluationContext context = new MethodBasedEvaluationContext(null, ((MethodSignature) pjp.getSignature()).getMethod(),
                pjp.getArgs(), new DefaultParameterNameDiscoverer());

        Map<String, CircuitBreakerConfigurationProperties.BackendProperties> backends = properties.getBackends();

        String backendName = (String) context.lookupVariable("backend");

        if (!backends.containsKey(backendName)) {
            CircuitBreakerConfigurationProperties.BackendProperties backendProperties = new CircuitBreakerConfigurationProperties.BackendProperties();
            CircuitBreakerConfig circuitBreakerConfig = properties.createCircuitBreakerConfig(backendName);
            CircuitBreaker circuitBreaker = registry.circuitBreaker(backendName, circuitBreakerConfig);
            circuitBreaker.getEventPublisher().onEvent(eventConsumerRegistry.createEventConsumer(backendName, backendProperties.getEventConsumerBufferSize()));

            backends.put(backendName, backendProperties);
        }

        return pjp.proceed();
    }
}
