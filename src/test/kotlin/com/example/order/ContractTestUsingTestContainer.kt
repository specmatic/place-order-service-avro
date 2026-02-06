package com.example.order

import SchemaRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.boot.test.context.SpringBootTest
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.PullPolicy.alwaysPull


@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ContractTestsUsingTestContainer {

    private val schemaRegistryContainer = SchemaRegistry.getContainer()
    private val mockContainer: GenericContainer<*> =
        GenericContainer("specmatic/enterprise")
            .withImagePullPolicy(alwaysPull())
            .withCommand("mock")
            .withNetworkMode("host")
            .withFileSystemBind(
                "./specmatic.yaml", "/usr/src/app/specmatic.yaml", BindMode.READ_ONLY,
            ).withFileSystemBind(
                "./build/reports/specmatic", "/usr/src/app/build/reports/specmatic", BindMode.READ_WRITE,
            ).waitingFor(Wait.forLogMessage(".*AsyncMock has started.*", 1))
            .withLogConsumer { print(it.utf8String) }

    private val testContainer: GenericContainer<*> =
        GenericContainer("specmatic/enterprise")
            .withImagePullPolicy(alwaysPull())
            .withCommand("test")
            .withNetworkMode("host")
            .withFileSystemBind(
                "./specmatic.yaml", "/usr/src/app/specmatic.yaml", BindMode.READ_ONLY,
            ).withFileSystemBind(
                "./build/reports/specmatic", "/usr/src/app/build/reports/specmatic", BindMode.READ_WRITE,
            ).waitingFor(Wait.forLogMessage(".*Tests run:.*", 1))
            .withLogConsumer { print(it.utf8String) }

    @BeforeAll
    fun setup() {
        schemaRegistryContainer.start()
    }

    @AfterAll
    fun tearDown() {
        schemaRegistryContainer.stop()
    }

    @Test
    fun specmaticContractTest() {
        try {
            mockContainer.start()
            testContainer.start()
            val hasSucceeded = testContainer.logs.contains("Failures: 0")
            assertThat(hasSucceeded).isTrue()
        } finally {
            // wait for message to be published on new-orders topic
            Thread.sleep(3000)
            mockContainer.execInContainer(
                "curl",
                "-sS",
                "-X",
                "POST",
                "http://localhost:9999/stop"
            )
            mockContainer.stop()
        }
    }
}
