package com.example.order

import SchemaRegistry
import io.specmatic.enterprise.SpecmaticContractTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ContractTest : SpecmaticContractTest {
    private val schemaRegistryContainer = SchemaRegistry.getContainer()

    @BeforeAll
    fun setup() {
        schemaRegistryContainer.start()
    }

    @AfterAll
    fun tearDown() {
        schemaRegistryContainer.stop()
    }
}