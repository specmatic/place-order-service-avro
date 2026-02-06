import org.testcontainers.containers.ComposeContainer
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy
import java.io.File
import java.time.Duration

class SchemaRegistry {
    companion object {
        private val DOCKER_COMPOSE_FILE = File("docker-compose.yaml")
        private const val REGISTER_SCHEMAS_SERVICE = "register-schemas"
        private const val SCHEMA_REGISTERED_REGEX = ".*(?i)schemas registered.*"

        fun getContainer(): ComposeContainer {
            return ComposeContainer(DOCKER_COMPOSE_FILE)
                .withLocalCompose(true)
                .waitingFor(
                    REGISTER_SCHEMAS_SERVICE,
                    LogMessageWaitStrategy()
                        .withRegEx(SCHEMA_REGISTERED_REGEX)
                        .withStartupTimeout(Duration.ofSeconds(60))
                )
        }
    }
}
