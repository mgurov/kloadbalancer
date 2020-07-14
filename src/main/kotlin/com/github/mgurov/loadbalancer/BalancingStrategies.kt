package com.github.mgurov.loadbalancer

import java.util.*
import java.util.concurrent.ThreadLocalRandom

/**
 *
 * An implementation of a load balancing algorithm.
 *
 * Implementation notes:
 *
 * If provided as a library with little control over the clients of the LoadBalancer,
 * we might want to hide this interface so that it's signature isn't a part of the public contract should we want to implement
 * more elaborate strategies in the future that require more knowledge about the active options number.
 *
 */
interface BalancingStrategy {
    /**
     * selectNextIndex should return the 0-based index of the next option (Provider) based on the strategy.
     *
     * The strategy may keep a state between the invocations.
     *
     * The invocations are supposed to be serialized by the caller, e.g. no two simultaneous calls would be made from different threads.
     */
    fun selectNextIndex(optionsCount: Int): Int
}

class RandomBalancingStrategy(
        private val randomSupplier: () -> Random = ThreadLocalRandom::current
) : BalancingStrategy {
    override fun selectNextIndex(optionsCount: Int): Int {
        return randomSupplier().nextInt(optionsCount)
    }
}

// a simplified RoundRobin that doesn't track the changes in the list of the available nodes
class RoundRobinBalancingStrategy(
        private var nextPosition: Int = 0
) : BalancingStrategy {
    override fun selectNextIndex(optionsCount: Int): Int {
        val selectedOption = nextPosition % optionsCount
        nextPosition = (nextPosition + 1) % optionsCount
        return selectedOption
    }
}
