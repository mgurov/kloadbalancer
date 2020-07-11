package com.github.mgurov.loadbalancer

class LoadBalancer {
    private val providers: List<Provider> = mutableListOf() //TODO: thread unsafe yet
    fun register(provider: Provider) {
        TODO("register checking the size")
    }
}