(ns lcmf.bus-test
  (:require [cljs.test :refer-macros [deftest is]]
            [lcmf.bus :as bus]))

(defn- thrown-ex [f]
  (try
    (f)
    nil
    (catch :default ex
      ex)))

(deftest publish-and-subscribe-test
  (let [app-bus (bus/make-bus)
        received (atom nil)]
    (bus/subscribe! app-bus :booking/created
                    (fn [_ envelope]
                      (reset! received envelope)))
    (let [envelope (bus/publish! app-bus
                                 :booking/created
                                 {:id "b-1"}
                                 {:module :booking})]
      (is (= :booking/created (:event-type envelope)))
      (is (= :booking (:module envelope)))
      (is (string? (:message-id envelope)))
      (is (string? (:correlation-id envelope)))
      (is (= [] (:causation-path envelope))))
    (is (= {:id "b-1"} (:payload @received)))))

(deftest module-is-required-test
  (let [app-bus (bus/make-bus)
        ex (thrown-ex #(bus/publish! app-bus :booking/created {} {}))]
    (is (some? ex))
    (is (= :missing-module (:reason (ex-data ex))))
    (is (re-find #"Missing required :module" (ex-message ex)))))

(deftest multiple-subscribers-and-error-isolation-test
  (let [app-bus (bus/make-bus)
        calls (atom [])]
    (bus/subscribe! app-bus :booking/created
                    (fn [_ _]
                      (swap! calls conj :bad)
                      (throw (ex-info "boom" {}))))
    (bus/subscribe! app-bus :booking/created
                    (fn [_ _]
                      (swap! calls conj :good)))
    (bus/publish! app-bus :booking/created {:id "b-1"} {:module :booking})
    (is (= [:bad :good] @calls))))

(deftest causation-and-correlation-test
  (let [app-bus (bus/make-bus)
        child (atom nil)
        root (atom nil)]
    (bus/subscribe! app-bus :notify/booking-created
                    (fn [_ envelope]
                      (reset! child envelope)))
    (bus/subscribe! app-bus :booking/created
                    (fn [b envelope]
                      (reset! root envelope)
                      (bus/publish! b
                                    :notify/booking-created
                                    {:booking-id "b-1"}
                                    {:module :notify
                                     :parent-envelope envelope})))
    (bus/publish! app-bus :booking/created {:id "b-1"} {:module :booking})
    (is (= (:correlation-id @root) (:correlation-id @child)))
    (is (= [[:booking :booking/created]] (:causation-path @child)))))

(deftest cycle-detection-test
  (let [app-bus (bus/make-bus)
        root-envelope (bus/publish! app-bus :booking/created {:id "b-1"} {:module :booking})
        ex (thrown-ex #(bus/publish! app-bus
                                     :booking/created
                                     {:id "b-2"}
                                     {:module :booking
                                      :parent-envelope root-envelope}))]
    (is (some? ex))
    (is (= :cycle-detected (:reason (ex-data ex))))
    (is (re-find #"Cycle detected" (ex-message ex)))))

(deftest max-depth-test
  (let [app-bus (bus/make-bus :max-depth 1)
        first-child (bus/publish! app-bus :event/one 1 {:module :m1})
        second-child (bus/publish! app-bus :event/two 2 {:module :m2 :parent-envelope first-child})
        ex (thrown-ex #(bus/publish! app-bus
                                     :event/three
                                     3
                                     {:module :m3
                                      :parent-envelope second-child}))]
    (is (some? ex))
    (is (= :max-depth-exceeded (:reason (ex-data ex))))
    (is (re-find #"Max causation depth exceeded" (ex-message ex)))))

(deftest unsubscribe-and-listener-count-test
  (let [app-bus (bus/make-bus)
        calls (atom 0)
        handler-1 (fn [_ _] (swap! calls inc))
        handler-2 (fn [_ _] (swap! calls inc))
        id-1 (bus/subscribe! app-bus :event/inc handler-1)
        _id-2 (bus/subscribe! app-bus :event/inc handler-2 {:meta {:slot :secondary}})]
    (is (= 2 (bus/listener-count app-bus :event/inc)))
    (is (= 2 (bus/listener-count app-bus)))
    (is (true? (bus/unsubscribe! app-bus :event/inc id-1)))
    (is (= 1 (bus/listener-count app-bus :event/inc)))
    (is (false? (bus/unsubscribe! app-bus :event/inc handler-1)))
    (is (true? (bus/unsubscribe! app-bus :event/inc handler-2)))
    (is (= 0 (bus/listener-count app-bus :event/inc)))
    (bus/subscribe! app-bus :event/inc handler-2 {:meta {:slot :secondary}})
    (is (true? (bus/unsubscribe! app-bus :event/inc {:slot :secondary})))
    (is (= 0 (bus/listener-count app-bus :event/inc)))
    (bus/publish! app-bus :event/inc nil {:module :test})
    (is (zero? @calls))))

(deftest logger-failure-does-not-break-publish-test
  (let [app-bus (bus/make-bus :logger (fn [_level data]
                                        (when (= :event-published (:event data))
                                          (throw (ex-info "logger boom" {})))))]
    (is (map?
         (bus/publish! app-bus :event/log nil {:module :test})))))
