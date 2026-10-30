package io.github.ayomide1x.objectstore.coordinator;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class GcConfig {

    /**
     * Wall-clock time for every timestamp garbage collection compares: when an
     * upload was initiated, when an object was retired, and "now". One clock
     * for all of them, injectable so grace periods can be tested without
     * sleeping. (Liveness uses the monotonic {@link Ticker}: its intervals are
     * never stored.)
     */
    @Bean
    Clock systemClock() {
        return Clock.systemUTC();
    }
}
