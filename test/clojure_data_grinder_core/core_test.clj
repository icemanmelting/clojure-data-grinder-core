(ns clojure-data-grinder-core.core-test
  (:require [clojure.test :refer :all]
            [clojure-data-grinder-core.core :refer :all]
            [clojure.core.async :as async]
            [clojure.java.io :as io]))

(def ^:private function-state (atom true))
(def ^:private sink-output (atom nil))

(defn- source-function []
  (when @function-state
    (reset! function-state false)
    "this is a test"))

(defn- grind-function [v]
  (when @function-state
    (reset! function-state false)
    (* v 2)))

(defn sink-fn [v]
  (when @function-state
    (reset! function-state false)
    (reset! sink-output (str "result -> " v))))

(defn cleaning-fixture [f]
  (reset! sink-output nil)
  (reset! function-state true)
  (reset-pool)
  (f))

(use-fixtures :each cleaning-fixture)

(defn <!!?
  "Reads from chan synchronously, waiting for a given maximum of milliseconds.
  If the value does not come in during that period, returns :timed-out. If
  milliseconds is not given, a default of 1000 is used."
  ([chan]
   (<!!? chan 1000))
  ([chan milliseconds]
   (let [timeout (async/timeout milliseconds)
         [value port] (async/alts!! [chan timeout])]
     (if (= chan port)
       value
       :timed-out))))

(deftest source-test
  (let [test-channel (async/chan 1)
        st (atom {:processed-batches 0
                  :successful-batches 0
                  :unsuccessful-batches 0})
        source (->SourceImpl st "test" {} nil source-function [test-channel] 5)]

    (.run source)

    (let [v (<!!? test-channel 2000)
          {pb :processed-batches sb :successful-batches} (.getState source)]
      (are [x y] (= x y)
        "this is a test" v
        pb 1
        sb 1))))

(deftest grinder-test
  (let [test-channel (async/chan 1)
        test-channel-2 (async/chan 1)
        st (atom {:processed-batches 0
                  :successful-batches 0
                  :unsuccessful-batches 0})
        grinder (->GrinderImpl st "test" {} nil test-channel grind-function [test-channel-2] 5)]
    (async/>!! test-channel 1)

    (.run grinder)

    (let [value (<!!? test-channel-2 2000)
          {pb :processed-batches sb :successful-batches} (.getState grinder)]
      (are [x y] (= x y)
        2 value
        1 pb
        1 sb))))

(deftest sink-test
  (let [test-channel (async/chan 1)
        state (atom {:processed-batches 0
                     :successful-batches 0
                     :unsuccessful-batches 0})
        sink (->SinkImpl state "test" {} nil sink-fn test-channel 5)]
    (async/>!! test-channel 1)

    (.run sink)

    (Thread/sleep 2000)

    (let [{pb :processed-batches sb :successful-batches} (.getState sink)]
      (are [x y] (= x y)
        "result -> 1" @sink-output
        pb 1
        sb 1))))

(deftest enricher-test
  (let [test-channel (async/chan 1)
        test-channel-2 (async/chan 1)
        state (atom {:processed-batches 0
                     :successful-batches 0
                     :unsuccessful-batches 0})
        enricher (->EnricherImpl state
                                 "test"
                                 {}
                                 nil
                                 test-channel
                                 (fn [cache v]
                                   (let [a (:a v)]
                                     (assoc v a (get cache a))))
                                 [test-channel-2]
                                 5
                                 (atom {:a "2"})
                                 nil
                                 5)]
    (async/>!! test-channel {:a :a})

    (.run enricher)

    (Thread/sleep 2000)

    (let [value (<!!? test-channel-2 2000)
          {pb :processed-batches sb :successful-batches} (.getState enricher)]
      (are [x y] (= x y)
        {:a "2"} value
        1 pb
        1 sb))))
