package com.github.mgurov.loadbalancer

class Provider(
        val id: String
) {
    fun get(): String = id
}