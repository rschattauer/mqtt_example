package com.example.mqttexampletest

interface Platform {
    val name: String
}

expect fun getPlatform(): Platform