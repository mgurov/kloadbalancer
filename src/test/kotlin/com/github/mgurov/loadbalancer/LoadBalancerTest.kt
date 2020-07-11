package com.github.mgurov.loadbalancer

import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.SoftAssertions
import org.junit.jupiter.api.Test
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
}