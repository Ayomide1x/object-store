package io.github.ayomide1x.objectstore.coordinator;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class LivenessConfig {

    @Bean
    Ticker systemTicker() {
        return System::nanoTime;
    }
}
