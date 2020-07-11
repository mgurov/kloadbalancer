package com.github.mgurov.loadbalancer

//TODO: make an interface
class Provider(
        val id: String
) {
    fun get(): String = id
}