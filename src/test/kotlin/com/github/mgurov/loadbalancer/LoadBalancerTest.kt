package com.github.mgurov.loadbalancer

import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.lang.RuntimeException
import kotlin.math.exp
import kotlin.random.Random

class LoadBalancerTest {
    @Test
    fun `should reject providers exceeding capacity`() {
        val loadBalancer = LoadBalancer(capacity = 2)
        loadBalancer.register(Provider("first provider"))
        loadBalancer.register(Provider("second provider"))

        Assertions.assertThatIllegalStateException().isThrownBy {
            loadBalancer.register(Provider("third provides exceeds the capacity of 2"))
        }
    }

    @Test
    fun `should call random providers when configured with such strategy`() {
        val loadBalancer = LoadBalancer(balancingStrategy = RandomBalancingStrategy(Random(0L)))
        loadBalancer.register(Provider("1"))
        loadBalancer.register(Provider("2"))

        val actuals = (1..10).map { loadBalancer.get() }

        assertThat(actuals).containsExactly("2", "1", "2", "2", "2", "1", "1", "1", "1", "2")
    }

    @Test
    fun `should call providers sequentially when configured with such strategy`() {
        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())
        loadBalancer.register(Provider("1"))
        loadBalancer.register(Provider("2"))

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
        loadBalancer.register(Provider("1"))
        loadBalancer.register(Provider("2"))

        assertThatCallsReturn(loadBalancer, 3, "1", "2", "1")

        //when
        loadBalancer.register(Provider("3"))
        //then
        assertThatCallsReturn(loadBalancer, 4, "2", "3", "1", "2")
    }

    @Test
    fun `should be possible to remove providers`() {

        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())
        loadBalancer.register(Provider("1"))
        val secondProvider = Provider("2")
        loadBalancer.register(secondProvider)

        assertThatCallsReturn(loadBalancer, 3, "1", "2", "1")

        //when
        assertThat(loadBalancer.unregister(secondProvider)).isTrue()
        assertThat(loadBalancer.unregister(secondProvider)).isFalse()
        //then
        assertThatCallsReturn(loadBalancer, 4, "1", "1", "1", "1")
    }
}

//TODO: fancier assertions maybe
private fun assertThatCallsReturn(loadBalancer: LoadBalancer, upTo: Int, vararg expected: String) {
    val actuals = (1..upTo).map { loadBalancer.get() }
    assertThat(actuals).containsExactly(*expected)
}
