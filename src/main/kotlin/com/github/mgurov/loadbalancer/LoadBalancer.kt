package com.github.mgurov.loadbalancer

class LoadBalancer(
        val capacity: Int = 10 //max number of providers allowed to be registered
) {
    private val providers: MutableList<Provider> = mutableListOf() //TODO: thread unsafe yet
    fun register(provider: Provider) {
        check(providers.size < capacity) {
            "Can't register more than $capacity providers"
        }
        providers += provider //TODO: thread safety
    }
}