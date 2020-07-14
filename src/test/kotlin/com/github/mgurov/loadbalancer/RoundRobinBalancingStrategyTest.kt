package com.github.mgurov.loadbalancer

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
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

    @Test
    fun `should work ok with singleton`() {
        val roundRobin = RoundRobinBalancingStrategy()

        assertThat(
                listOf(
                        roundRobin.selectNextIndex(1),
                        roundRobin.selectNextIndex(1)
                )
        ).containsExactly(0, 0)
    }

    @Test
    fun `ok to throw division by zero on no options since shouldn't be called this way`() {
        val roundRobin = RoundRobinBalancingStrategy()

        assertThatExceptionOfType(ArithmeticException::class.java).isThrownBy {
            roundRobin.selectNextIndex(0)
        }
    }

}