package com.github.mgurov.loadbalancer

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

class LoadBalancerTest {
    @Test
    fun `should reject providers exceeding capacity`() {
        //given
        val loadBalancer = LoadBalancer(capacity = 2)
        loadBalancer.register(Provider("first provider"))
        loadBalancer.register(Provider("second provider"))
        //when-then
        Assertions.assertThatIllegalStateException().isThrownBy {
            loadBalancer.register(Provider("exceeding the capacity of 2"))
        }
    }
}