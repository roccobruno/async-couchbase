package org.asyncouchbase.util

import play.api.libs.iteratee.Concurrent.Channel
import play.api.libs.iteratee._
import rx.lang.scala.JavaConversions._
import rx.lang.scala.{Observable, Observer, Subscription}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Converters {

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
     * Enumerator to Observable
     */
    implicit def enumerator2Observable[T](enum: Enumerator[T]): Observable[T] = {
      // creating the Observable that we return
      Observable({ observer: Observer[T] =>
        // keeping a way to unsubscribe from the observable
        var cancelled = false

        // enumerator input is tested with this predicate
        // once cancelled is set to true, the enumerator will stop producing data
        val cancellableEnum = enum through Enumeratee.breakE[T](_ => cancelled)

        // applying iteratee on producer, passing data to the observable
        cancellableEnum (
          Iteratee.foreach(observer.onNext(_))
        ).onComplete { // passing completion or error to the observable
          case Success(_) => observer.onCompleted()
          case Failure(e) => observer.onError(e)
        }

        // unsubscription will change the var to stop the enumerator above via the breakE function
        new Subscription { override def unsubscribe() = { cancelled = true } }
      })
    }


  /*
   Observable to Future
   */
  //  implicit def toFuture[T](observable: rx.Observable[T]) = toScalaObservable(observable).toBlocking.toFuture
  implicit def toFuture[T](observable: rx.Observable[T]) = {

   observable2Enumerator(observable) run
      Iteratee.fold(List.empty[T]) { (l, e) => e :: l } map {
      case head :: tail => head
    }

  }


}
