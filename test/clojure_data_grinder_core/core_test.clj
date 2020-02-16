(ns clojure-data-grinder-core.core-test
  (:require [clojure.test :refer :all]
            [clojure-data-grinder-core.core :refer :all]
            [clojure.core.async :as async]
            [clojure.java.io :as io])
  (:import (java.io IOException)))

(def function-state (atom true))
(def output-file-name "test.txt")

(def test-channel (async/chan 1))
(def test-channel-2 (async/chan 1))

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
    (spit output-file-name (str "result -> " v))))

(defn cleaning-fixture [f]
  (if (.exists (io/as-file output-file-name))
    (io/delete-file output-file-name true))
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
  (let [st (atom {:processed-batches 0
                     :successful-batches 0
                     :unsuccessful-batches 0})
        source (->SourceImpl st "test" {} nil source-function test-channel 5)]

    (.init source)

    (let [v (<!!? test-channel 2000)
          {pb :processed-batches sb :successful-batches} (.getState source)]
      (are [x y] (= x y)
        "this is a test" v
        pb 1
        sb 1))))

(deftest grinder-test
  (let [st (atom {:processed-batches 0
                     :successful-batches 0
                     :unsuccessful-batches 0})
        grinder (->GrinderImpl st "test" {} nil test-channel grind-function test-channel-2 5)]
    (async/>!! test-channel 1)

    (.init grinder)

    (let [value (<!!? test-channel-2 2000)
          {pb :processed-batches sb :successful-batches} (.getState grinder)]
      (are [x y] (= x y)
        2 value
        1 pb
        1 sb))))

;(deftest sink-test
;  (let [state (atom {:processed-batches 0
;                     :successful-batches 0
;                     :unsuccessful-batches 0})
;        sink (->SinkImpl state "test" {} nil sink-fn test-channel 5)]
;    (async/>!! test-channel 1)
;
;    (.init sink)
;
;    (Thread/sleep 2000)
;
;    (let [{pb :processed-batches sb :successful-batches} (.getState sink)]
;          ;file-content (slurp output-file-name)]
;      (are [x y] (= x y)
;       ; file-content "result -> 1"
;        pb 1
;        sb 1))))
