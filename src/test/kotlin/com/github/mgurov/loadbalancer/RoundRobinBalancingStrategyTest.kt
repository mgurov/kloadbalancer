package com.github.mgurov.loadbalancer

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RoundRobinBalancingStrategyTest {
    @Test
    fun `picks first position by default`() {
        assertThat(
            RoundRobinBalancingStrategy().selectNextIndex(3)
        ).isEqualTo(0)
    }

    @Test
    fun `should pick current position`() {
        assertThat(
            RoundRobinBalancingStrategy(position = 1).selectNextIndex(3)
        ).isEqualTo(1)
    }

    @Test
    fun `should start anew when exceeding the list`() {
        val roundRobin = RoundRobinBalancingStrategy()

        assertThat(
                listOf(
                        roundRobin.selectNextIndex(3),
                        roundRobin.selectNextIndex(3),
                        roundRobin.selectNextIndex(3),
                        roundRobin.selectNextIndex(3)
                )
        ).containsExactly(0, 1, 2, 0)
    }

}