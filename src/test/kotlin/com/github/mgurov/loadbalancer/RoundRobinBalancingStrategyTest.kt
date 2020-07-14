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
            RoundRobinBalancingStrategy(nextPosition = 1).selectNextIndex(3)
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

    @Test
    fun `should start anew when appeared to exceed the size`() {
        val roundRobin = RoundRobinBalancingStrategy()

        assertThat(
                listOf(
                        roundRobin.selectNextIndex(3),
                        roundRobin.selectNextIndex(3),
                        roundRobin.selectNextIndex(2), //one active item less
                        roundRobin.selectNextIndex(2)
                )
        ).containsExactly(0, 1, 0, 1)
    }

    @Test
    fun `should continue when extra items added`() {
        val roundRobin = RoundRobinBalancingStrategy()

        assertThat(
                listOf(
                        roundRobin.selectNextIndex(3),
                        roundRobin.selectNextIndex(3),
                        roundRobin.selectNextIndex(3),
                        roundRobin.selectNextIndex(4),  //one active item more
                        roundRobin.selectNextIndex(4),
                        roundRobin.selectNextIndex(4),
                        roundRobin.selectNextIndex(4),
                        roundRobin.selectNextIndex(4)
                )
        ).containsExactly(0, 1, 2, 0, 1, 2, 3, 0)
    }

    //TODO: 1's
    //TODO: zero's

}