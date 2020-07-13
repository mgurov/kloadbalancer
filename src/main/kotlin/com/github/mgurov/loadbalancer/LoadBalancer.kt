package com.github.mgurov.loadbalancer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write
import kotlin.random.Random

class LoadBalancer(
        val capacity: Int = 10, //max number of providers allowed to be registered
        val balancingStrategy: BalancingStrategy = RandomBalancingStrategy(),
        val simultaneousCallSingleProviderLimit: Int = Integer.MAX_VALUE //TODO: document?
) {
    private val lock = ReentrantReadWriteLock()
    private val pickerLock = ReentrantLock()
    @Volatile //TODO: do I still need it?
    private var providers: List<ProviderStatusHolder> = listOf() //TODO: thread unsafe yet

    //TODO: document up to client to ensure uniqueness of the providers.
    //TODO: document assumes infrequent modifications
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
    //TODO: describe since we're reading from providers, those shall stay intact
    //TODO: mention single thread
    fun checkProvidersHealth() {
        lock.read {
            providers.forEach {
                it.check()
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
        val chosenProviderIndex = pickerLock.withLock {
            balancingStrategy.selectNextIndex(activeProviders.size)
        }
        return activeProviders[chosenProviderIndex].get()
    }

    //TODO: make data class with copying
    //TODO: wrap as "provider wrapper" interface?
    //TODO: make private parts?
    class ProviderStatusHolder(
            val provider: Provider,
            @Volatile
            var status: ProviderStatus,
            var callsInProgress: AtomicInteger = AtomicInteger(0)
    ) {
        fun get(): String {
            try {
                callsInProgress.incrementAndGet()
                return provider.get()
            } finally {
                callsInProgress.decrementAndGet()
            }
        }

        fun check() {
            status = if (provider.check()) {
                if (status == ProviderStatus.NOK) {
                    ProviderStatus.RECOVERING
                } else {
                    ProviderStatus.OK
                }
            } else {
                ProviderStatus.NOK
            }
        }
    }

    enum class ProviderStatus{
        OK,
        RECOVERING,
        NOK
    }
}

//TODO: describe supposed to be called serially
interface BalancingStrategy {
    /**
     * TODO: optionsCount > 0
     */
    fun selectNextIndex(optionsCount: Int): Int
}

class RandomBalancingStrategy(
        private val random: Random = Random.Default
): BalancingStrategy {
    override fun selectNextIndex(optionsCount: Int): Int {
        return random.nextInt(optionsCount) //TODO: thread unsafe re. random
    }
}

//TODO: describe simplified approach and when we'd want to use a more complicated - when want to real track which was previous and not.
class RoundRobinBalancingStrategy(
        private var position: Int = 0
): BalancingStrategy {
    override fun selectNextIndex(optionsCount: Int): Int {
        val theNextOne = position % optionsCount
        position = (position + 1) % optionsCount
        return theNextOne
   }
}

//TODO: mention potential performance benefit of lambda's