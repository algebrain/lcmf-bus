(ns lcmf.bus-test-runner
  (:require [clojure.test :as t]
            [lcmf.bus-test]))

(defn -main []
  (let [{:keys [fail error]} (t/run-tests 'lcmf.bus-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
