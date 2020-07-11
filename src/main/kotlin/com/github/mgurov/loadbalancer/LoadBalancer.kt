package com.github.mgurov.loadbalancer

import kotlin.random.Random

class LoadBalancer(
        val capacity: Int = 10, //max number of providers allowed to be registered
        val balancingStrategy: BalancingStrategy = RandomBalancingStrategy()
) {
    //TODO: volatile
    private var providers: List<ProviderStatusHolder> = listOf() //TODO: thread unsafe yet

    //TODO: document up to client to ensure uniqueness of the providers.
    fun register(provider: Provider) {
        check(providers.size < capacity) {
            "Can't register more than $capacity providers"
        }
        providers = providers + ProviderStatusHolder(provider, status = ProviderStatus.OK)
    }

    //TODO: document first only and by equality
    //TODO: document returns true if unregistered false if not found
    fun unregister(provider: Provider) {
        providers = providers.filter { it.provider != provider }
    }

    //TODO: make it be executed periodically (every X sec)
    //TODO: make it immutable maybe.
    fun checkProvidersHealth() {
        providers.forEach {
            if (it.provider.check()) {
                if (it.status == ProviderStatus.NOK) {
                    it.status = ProviderStatus.RECOVERING
                } else {
                    it.status = ProviderStatus.OK
                }
            } else {
                it.status = ProviderStatus.NOK
            }
        }
    }

    //TODO: describe can return null if no backing providers available
    fun get(): String? {
        val activeProviders = providers.filter { it.status == ProviderStatus.OK }.map { it.provider }
        if (activeProviders.isEmpty()) {
            return null
        }
        //TODO: more performance effective way
        return balancingStrategy.selectNext(activeProviders).get()
    }

    //TODO: make data class with copying
    private class ProviderStatusHolder(
            val provider: Provider,
            var status: ProviderStatus
    )

    private enum class ProviderStatus{
        OK,
        RECOVERING,
        NOK
    }
}

interface BalancingStrategy {
    /**
     * should not be called with empty list of providers
     */
    fun selectNext(providers: List<Provider>): Provider
}

class RandomBalancingStrategy(
        private val random: Random = Random.Default
): BalancingStrategy {
    override fun selectNext(providers: List<Provider>): Provider {
        return providers[random.nextInt(providers.size)] //TODO: thread unsafe re. random
    }
}

class RoundRobinBalancingStrategy(
        private var position: Int = 0
): BalancingStrategy {
    override fun selectNext(providers: List<Provider>): Provider {
        val theNextOne = providers[position % providers.size] //TODO: take care of the empty length
        position = (position + 1) % providers.size
        return theNextOne
   }
}