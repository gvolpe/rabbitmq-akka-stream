package com.gvolpe.rabbitstream

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.spingo.op_rabbit.Directives._
import com.spingo.op_rabbit.PlayJsonSupport._
import com.spingo.op_rabbit.stream.{MessagePublisherSink, RabbitSource}
import com.spingo.op_rabbit.{RabbitControl, _}
import com.timcharper.acked.AckedSource
import play.api.libs.json._

object Client extends App {

  implicit val actorSystem = ActorSystem("rabbitSystem")
  implicit val actorMaterializer = ActorMaterializer()
  val rabbitControl = actorSystem.actorOf(Props[RabbitControl])

  case class Person(name: String, age: Int)

  implicit val personFormat = Json.format[Person] // setup play-json serializer

  val personList = (1 to 10).map(n => Person(s"Agent #$n", n))

  // Publisher
  AckedSource(personList). // Each element in source will be acknowledged after publish confirmation is received
    map(Message.queue(_, "test-queue")).
      to(MessagePublisherSink(rabbitControl))
      .run

  // Consumer
  RabbitSource(
    rabbitControl,
    channel(qos = 3),
    consume(queue("test-queue", durable = true, exclusive = false, autoDelete = false)),
    body(as[Person])). // marshalling is automatically hooked up using implicits
      runForeach { person =>
        println(person)
      } // after each successful iteration the message is acknowledged.

}
