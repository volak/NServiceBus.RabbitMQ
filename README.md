# RabbitMqNext Transport for NServiceBus

This is a fork of the official rabbitmq transport at [Particular/NServiceBus.RabbitMQ](https://github.com/Particular/NServiceBus.RabbitMQ) except this version uses the independent RabbitMq client [clearctvm/RabbitMqNext](https://github.com/clearctvm/RabbitMqNext)

## Why?
Well the reason why RabbitMqNext exists is because they found the main Rabbit client bottlenecking their apps with all the blocking threads.  If you check the concurrency visualizer while running the official client you'll notice ~90% of your app's time is spent waiting.  RabbitMqNext was built for async/await out of the box and you may find some performance boost by using this transport instead.

There have been many requests for an official async/await rabbit client - we're told to expect it Q2/Q3 2017.

## Installation

Exactly the same as NServiceBus.RabbitMQ - its a drop in replacement

