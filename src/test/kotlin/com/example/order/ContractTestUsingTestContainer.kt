package com.example.order

import io.specmatic.async.core.constants.AVAILABLE_SERVERS
import io.specmatic.async.core.constants.SCHEMA_REGISTRY_KIND
import io.specmatic.async.core.constants.SCHEMA_REGISTRY_URL
import io.specmatic.async.core.constants.SchemaRegistryKind
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.ComposeContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy
import org.testcontainers.containers.wait.strategy.Wait
import java.io.File
import java.time.Duration

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ContractTestUsingTestContainer {
    companion object {
        private val DOCKER_COMPOSE_FILE = File("docker-compose.yaml")
        private const val REGISTER_SCHEMAS_SERVICE = "register-schemas"
        private const val SCHEMA_REGISTERED_REGEX = ".*(?i)schemas registered.*"
    }

    @Value("\${spring.kafka.properties.schema.registry.url}")
    lateinit var schemaRegistryUrl: String

    @Value("\${spring.kafka.bootstrap-servers}")
    lateinit var kafkaBootstrapServers: String

    private val schemaRegistry = schemaRegistry()

    private fun schemaRegistry(): ComposeContainer {
        return ComposeContainer(DOCKER_COMPOSE_FILE)
            .withLocalCompose(true).waitingFor(
                REGISTER_SCHEMAS_SERVICE,
                LogMessageWaitStrategy()
                    .withRegEx(SCHEMA_REGISTERED_REGEX)
                    .withStartupTimeout(Duration.ofSeconds(60))
            )
    }

    @BeforeAll
    fun setup() {
        schemaRegistry.start()
    }

    @AfterAll
    fun tearDown() {
        schemaRegistry.stop()
    }

    private fun GenericContainer<*>.configure(waitLogMessage: String): GenericContainer<*> {
        return this.withEnv(
            mapOf(
                SCHEMA_REGISTRY_URL to schemaRegistryUrl,
                SCHEMA_REGISTRY_KIND to SchemaRegistryKind.CONFLUENT.name,
                AVAILABLE_SERVERS to kafkaBootstrapServers
            )
        )
            .withFileSystemBind(
                "./specmatic.yaml",
                "/usr/src/app/specmatic.yaml",
                BindMode.READ_ONLY,
            ).withFileSystemBind(
                "./build/reports/specmatic",
                "/usr/src/app/build/reports/specmatic",
                BindMode.READ_WRITE,
            ).waitingFor(Wait.forLogMessage(".*$waitLogMessage.*", 1))
            .withNetworkMode("host")
            .withLogConsumer { print(it.utf8String) }
    }

    private fun testContainer(): GenericContainer<*> {
        return GenericContainer("specmatic/specmatic-kafka")
            .withCommand(
                "test",
                "--broker=$kafkaBootstrapServers",
            ).configure("The coverage report is generated")
    }

    private fun stubContainer(): GenericContainer<*> {
        return GenericContainer("specmatic/specmatic-kafka")
            .withCommand(
                "virtualize",
                "--broker=$kafkaBootstrapServers",
            ).configure("KafkaMock has started")
    }

    @Test
    fun specmaticContractTest() {
        val stubContainer = stubContainer()
        val testContainer = testContainer()
        try {
            stubContainer.start()
            testContainer.start()
            val hasSucceeded = testContainer.logs.contains("Result: FAILED").not()
            assertThat(hasSucceeded).isTrue()
        } finally {
            // wait for message to be published on wip-orders topic
            Thread.sleep(3000)
            stubContainer.execInContainer(
                "curl",
                "-sS",
                "-X",
                "POST",
                "http://localhost:9999/stop"
            )
            stubContainer.stop()
        }
    }
}
