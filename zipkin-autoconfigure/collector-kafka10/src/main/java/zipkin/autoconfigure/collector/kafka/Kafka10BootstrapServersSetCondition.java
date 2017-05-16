/**
 * Copyright 2015-2016 The OpenZipkin Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.autoconfigure.collector.kafka;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * This condition passes when {@link Kafka10CollectorProperties#getBootstrapServers()} is set to
 * non-empty.
 *
 * <p>This is here because the yaml defaults this property to empty like this, and spring-boot
 * doesn't have an option to treat empty properties as unset.
 *
 * <pre>{@code
 * zookeeper: ${KAFKA_ZOOKEEPER:}
 * }</pre>
 */
final class Kafka10BootstrapServersSetCondition extends SpringBootCondition {
    private static final String PROPERTY_NAME = "zipkin.collector.kafka10.bootstrapServers";

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata a) {
        String bootstrapServers = context.getEnvironment().getProperty(PROPERTY_NAME);
        return bootstrapServers == null || bootstrapServers.isEmpty() ?
                ConditionOutcome.noMatch(PROPERTY_NAME + " isn't set") :
                ConditionOutcome.match();
    }
}
