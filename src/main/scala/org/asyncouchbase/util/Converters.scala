package org.asyncouchbase.util

import play.api.libs.iteratee.Concurrent.Channel
import play.api.libs.iteratee._
import rx.lang.scala.JavaConversions._
import rx.lang.scala.{Observable, Observer}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

package object Converters {

  /*
 * Observable to Enumerator
 */
  implicit def observable2Enumerator[T](obs: Observable[T]): Enumerator[T] = {
    // unicast create a channel where you can push data and returns an Enumerator
    Concurrent.unicast { channel =>
      val subscription = obs.subscribe(new ChannelObserver(channel))
      val onComplete = { () => subscription.unsubscribe }
      val onError = { (_: String, _: Input[T]) => subscription.unsubscribe }
      (onComplete, onError)
    }
  }

  class ChannelObserver[T](channel: Channel[T]) extends rx.lang.scala.Observer[T] {
    override def onNext(elem: T): Unit = channel.push(elem)

    override def onCompleted(): Unit = channel.end()

    override def onError(e: Throwable): Unit = channel.end(e)
  }

  /*
   Observable to Future
   */
  implicit def toFuture[T](observable: rx.Observable[T]): Future[T] = {

    val promise = Promise[T]
    observable.subscribe( new Observer[T] {
      override def onNext(value: T): Unit = promise.success(value)

      override def onError(error: Throwable): Unit = promise.failure(error)

      override def onCompleted(): Unit =
        if(!promise.isCompleted) promise.failure(new RuntimeException("No results returned from Observable"))
    } )
    promise.future
  }



}
