package com.github.mgurov.loadbalancer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.random.Random

class LoadBalancer(
        val capacity: Int = 10, //max number of providers allowed to be registered
        val balancingStrategy: BalancingStrategy = RandomBalancingStrategy(),
        val simultaneousCallSingleProviderLimit: Int = Integer.MAX_VALUE //TODO: document?
) {
    private val lock = ReentrantReadWriteLock()
    @Volatile //TODO: do I still need it?
    private var providers: List<ProviderStatusHolder> = listOf() //TODO: thread unsafe yet

    //TODO: document up to client to ensure uniqueness of the providers.
    fun register(provider: Provider) {
        lock.write {
            check(providers.size < capacity) {
                "Can't register more than $capacity providers"
            }
            providers = providers + ProviderStatusHolder(provider, status = ProviderStatus.OK)
        }
    }

    //TODO: document first only and by equality
    //TODO: document returns true if unregistered false if not found
    fun unregister(provider: Provider) {
        lock.write {
            providers = providers.filter { it.provider != provider }
        }
    }

    //TODO: make it be executed periodically (every X sec)
    //TODO: make it immutable maybe.
    //TODO: describe since we're reading from providers, those shall stay intact
    fun checkProvidersHealth() {
        lock.read {
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
    }

    //TODO: describe can return null if no backing providers available
    //TODO: describe "optimistic" selection
    fun get(): String? {
        //TODO: will it be OK with the timing issues?
        val activeProviders = lock.read {
            providers.filter { it.status == ProviderStatus.OK && it.callsInProgress.get() < simultaneousCallSingleProviderLimit }
        }
        if (activeProviders.isEmpty()) {
            return null
        }
        //TODO: more performance effective way
        val providerChosen = balancingStrategy.selectNext(activeProviders)
        return providerChosen.get()
    }

    //TODO: make data class with copying
    //TODO: wrap as "provider wrapper" interface?
    //TODO: make private parts?
    class ProviderStatusHolder(
            val provider: Provider,
            var status: ProviderStatus,
            var callsInProgress: AtomicInteger = AtomicInteger(0)
    ) {
        fun get(): String {
            try {
                println("entering " + callsInProgress.incrementAndGet())
                return provider.get()
            } finally {
                println("leaving " + callsInProgress.decrementAndGet())

            }
        }
    }

    enum class ProviderStatus{
        OK,
        RECOVERING,
        NOK
    }
}

interface BalancingStrategy {
    /**
     * should not be called with empty list of providers
     */
    fun selectNext(providers: List<LoadBalancer.ProviderStatusHolder>): LoadBalancer.ProviderStatusHolder
}

class RandomBalancingStrategy(
        private val random: Random = Random.Default
): BalancingStrategy {
    override fun selectNext(providers: List<LoadBalancer.ProviderStatusHolder>): LoadBalancer.ProviderStatusHolder {
        return providers[random.nextInt(providers.size)] //TODO: thread unsafe re. random
    }
}

class RoundRobinBalancingStrategy(
        private var position: Int = 0
): BalancingStrategy {
    override fun selectNext(providers: List<LoadBalancer.ProviderStatusHolder>): LoadBalancer.ProviderStatusHolder {
        val theNextOne = providers[position % providers.size] //TODO: take care of the empty length
        position = (position + 1) % providers.size
        return theNextOne
   }
}