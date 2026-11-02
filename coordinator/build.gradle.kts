import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    java
    id("org.springframework.boot") version "4.1.2"
    id("com.google.protobuf") version "0.9.6"
}

group = "io.github.ayomide1x"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform(SpringBootPlugin.BOM_COORDINATES))
    implementation("org.springframework.boot:spring-boot-starter-grpc-server")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    // Successful DNS lookups are never cached in tests, by the JVM
    // (sun.net.inetaddr.ttl) or by grpc-java's DNS resolver
    // (networkaddress.cache.ttl, read as a system property), so a test can
    // change what a name resolves to (TestDns) and see the next lookup follow.
    systemProperty("sun.net.inetaddr.ttl", "0")
    systemProperty("networkaddress.cache.ttl", "0")
}

// The .proto files live at the repo root and are shared with the Go code.
// The Spring Boot plugin aligns protoc and protoc-gen-grpc-java with the
// protobuf and gRPC versions on the runtime classpath, and applies the grpc
// plugin to every generateProto task.
sourceSets {
    main {
        proto {
            srcDir("../proto")
        }
    }
}

protobuf {
    plugins {
        create("grpc")
    }
}

tasks.bootJar {
    archiveFileName = "coordinator.jar"
}
