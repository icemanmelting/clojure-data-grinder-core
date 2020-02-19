(ns clojure-data-grinder-core.core
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as a :refer [chan go go-loop <!! >!! mult tap close!]]
            [overtone.at-at :as at]
            [juxt.dirwatch :refer [watch-dir]]))

(def schedule-pool (at/mk-pool))

(defn reset-pool []
  (at/stop-and-reset-pool! schedule-pool :strategy :kill))

(defprotocol Step
  "Base step that contains the methods common to all Steps in the processing pipeline"
  (init [this] "initialize the current step")
  (validate [this])
  (getState [this]))

(defprotocol Source
  "Source Step -> reads raw data ready to be processed"
  (output [this value] "method to output the sourced data"))

(defrecord SourceImpl [state name conf v-fn x-fn out poll-frequency-s]
  Source
  (output [this value]
    (log/debug "Adding value " value " to source channel " name)
    (>!! out value))                                        ;;todo - need to create logic for batch output, maybe method next batch???
  Step
  (init [this]
    (log/debug "Initialized Source " name)
    (at/every poll-frequency-s
              #(let [{sb :successful-batches ub :unsuccessful-batches pb :processed-batches} @state]
                 (try
                   (when-let [v (x-fn)]
                     (output this v)
                     (swap! state merge {:processed-batches (inc pb) :successful-batches (inc sb)}))
                   (catch Exception e
                     (log/error e)
                     (swap! state merge {:processed-batches (inc pb) :unsuccessful-batches (inc ub)}))))
              schedule-pool))
  (validate [this]
    (if-let [result (v-fn conf)]
      (throw (ex-info "Problem validating Source conf!" result))
      (log/debug "Source " name " validated")))
  (getState [this] @state)
  Runnable
  (^void run [this]
    (init this)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;Pre-implemented sources;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord FileWatcherSource [state name conf v-fn x-fn out poll-frequency-s]
  Source
  (output [this value]
    (log/debug "Adding value " value " to source channel " name)
    (>!! out value))
  Step
  (init [this]
    (log/debug "Initialized Source " name)
    (watch-dir #(>!! out %) (clojure.java.io/file (:watch-dir conf))))
  (validate [this]
    (if-let [result (v-fn conf)]
      (throw (ex-info "Problem validating Source conf!" result))
      (log/debug "Source " name " validated")))
  (getState [this] @state)
  Runnable
  (^void run [this]
    (init this)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol Grinder
  "Data Grinder -> processes the raw data"
  (grind [this v]))

(defrecord GrinderImpl [state name conf v-fn in x-fn out poll-frequency-s]
  Grinder
  (grind [this v]
    (log/debug "Grinding value " v " on Grinder " name)
    (when-let [res (x-fn v)]
      (>!! out res)))
  Step
  (init [this]
    (log/debug "Initialized Grinder " name)
    (at/every poll-frequency-s
              #(let [{sb :successful-batches ub :unsuccessful-batches pb :processed-batches} @state]
                 (try
                   (when-let [v (<!! in)]
                     (log/debug "I AM HERE!!")
                     (grind this v)
                     (swap! state merge {:processed-batches (inc pb) :successful-batches (inc sb)}))
                   (catch Exception e
                     (log/error e)
                     (swap! state merge {:processed-batches (inc pb) :unsuccessful-batches (inc ub)})))
                 (log/debug @state))
              schedule-pool))
  (validate [this]
    (if-let [result (v-fn conf)]
      (throw (ex-info "Problem validating Grinder conf!" result))
      (log/debug "Grinder " name " validated")))
  (getState [this] @state)
  Runnable
  (^void run [this]
    (init this)))

(defprotocol Sink
  "Data Sink -> sinks the data into whatever form needed, DB, File, Cloud, etc"
  (sink [this v] "method to sink data"))

(defrecord SinkImpl [state name conf v-fn x-fn in poll-frequency-s]
  Sink
  (sink [this v]
    (log/debug "Sinking value " v " to " name)
    (x-fn v))
  Step
  (validate [this]
    (if-let [result (v-fn conf)]
      (throw (ex-info "Problem validating Sink conf!" result))
      (log/debug "Sink " name " validated")))
  (init [this]
    (log/debug "Initialized Sink " name)
    (at/every poll-frequency-s
              #(let [{sb :successful-batches ub :unsuccessful-batches pb :processed-batches} @state]
                 (try
                   (when-let [v (<!! in)]
                     (sink this v)
                     (swap! state merge {:processed-batches (inc pb) :successful-batches (inc sb)}))
                   (catch Exception e
                     (log/error e)
                     (swap! state merge {:processed-batches (inc pb) :unsuccessful-batches (inc ub)}))))
              schedule-pool))
  (getState [this] @state)
  Runnable
  (^void run [this]
    (init this)))
