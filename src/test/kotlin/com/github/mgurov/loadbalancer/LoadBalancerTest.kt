package com.github.mgurov.loadbalancer

import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.lang.RuntimeException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.random.Random

class LoadBalancerTest {
    @Test
    fun `should reject providers exceeding capacity`() {
        val loadBalancer = LoadBalancer(capacity = 2)
        loadBalancer.register(TestProvider("first provider"))
        loadBalancer.register(TestProvider("second provider"))

        Assertions.assertThatIllegalStateException().isThrownBy {
            loadBalancer.register(TestProvider("third provides exceeds the capacity of 2"))
        }
    }

    @Test
    fun `should call random providers when configured with such strategy`() {
        val loadBalancer = LoadBalancer(balancingStrategy = RandomBalancingStrategy(Random(0L)))
        loadBalancer.register(TestProvider("1"))
        loadBalancer.register(TestProvider("2"))

        val actuals = (1..10).map { loadBalancer.get() }

        assertThat(actuals).containsExactly("2", "1", "2", "2", "2", "1", "1", "1", "1", "2")
    }

    @Test
    fun `should call providers sequentially when configured with such strategy`() {
        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())
        loadBalancer.register(TestProvider("1"))
        loadBalancer.register(TestProvider("2"))

        val actuals = (1..10).map { loadBalancer.get() }

        assertThat(actuals).containsExactly("1", "2", "1", "2", "1", "2", "1", "2", "1", "2")
    }

    @Test
    fun `should not invoke balancing strategy if no providers`() {
        val loadBalancer = LoadBalancer(balancingStrategy = object: BalancingStrategy {
            override fun selectNext(providers: List<Provider>): Provider {
                throw RuntimeException("should've not called me")
            }
        })

        assertThat(loadBalancer.get()).isNull()
    }

    @Test
    fun `should be possible to add providers`() {

        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())
        loadBalancer.register(TestProvider("1"))
        loadBalancer.register(TestProvider("2"))

        assertThatCallsReturn(loadBalancer, 3, "1", "2", "1")

        //when
        loadBalancer.register(TestProvider("3"))
        //then
        assertThatCallsReturn(loadBalancer, 4, "2", "3", "1", "2")
    }

    @Test
    fun `should be possible to remove providers`() {

        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())
        loadBalancer.register(TestProvider("1"))
        val secondProvider = TestProvider("2")
        loadBalancer.register(secondProvider)

        assertThatCallsReturn(loadBalancer, 3, "1", "2", "1")

        //when
        loadBalancer.unregister(secondProvider)
        //then
        assertThatCallsReturn(loadBalancer, 4, "1", "1", "1", "1")
    }

    @Test
    fun `health check should disable unhealthy nodes`() {

        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())
        loadBalancer.register(TestProvider("1"))
        val secondProvider = TestProvider("2")
        secondProvider.healthy.set(false)
        loadBalancer.register(secondProvider)

        assertThatCallsReturn(loadBalancer, 3, "1", "2", "1") //health not checked yet

        //when
        loadBalancer.checkProvidersHealth()
        //then
        assertThatCallsReturn(loadBalancer, 4, "1", "1", "1", "1")
    }
}

//TODO: fancier assertions maybe
private fun assertThatCallsReturn(loadBalancer: LoadBalancer, upTo: Int, vararg expected: String) {
    val actuals = (1..upTo).map { loadBalancer.get() }
    assertThat(actuals).containsExactly(*expected)
}

class TestProvider(
        val id: String,
        val healthy: AtomicBoolean = AtomicBoolean(true)
): Provider {
    override fun get(): String = id
    override fun check(): Boolean = healthy.get()
}