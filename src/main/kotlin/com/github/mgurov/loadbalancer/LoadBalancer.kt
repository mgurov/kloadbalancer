package com.github.mgurov.loadbalancer

import kotlin.random.Random

class LoadBalancer(
        val capacity: Int = 10, //max number of providers allowed to be registered
        val balancingStrategy: BalancingStrategy = RandomBalancingStrategy()
) {
    private val providers: MutableList<Provider> = mutableListOf() //TODO: thread unsafe yet
    fun register(provider: Provider) {
        check(providers.size < capacity) {
            "Can't register more than $capacity providers"
        }
        providers += provider //TODO: thread safety
    }

    fun get(): String {
        return balancingStrategy.selectNext(providers).get()
    }
}

interface BalancingStrategy {
    fun selectNext(providers: List<Provider>): Provider
}

class RandomBalancingStrategy(
        private val random: Random = Random.Default
): BalancingStrategy {
    override fun selectNext(providers: List<Provider>): Provider {
        return providers[random.nextInt(providers.size)] //TODO: thread unsafe re. random
    }
}