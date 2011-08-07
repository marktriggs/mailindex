(ns net.dishevelled.test.mailindex
  (:require [clojure.contrib.seq :as sq])
  (:import (org.apache.lucene.store RAMDirectory))
  (:use clojure.test
        net.dishevelled.mailindex))



(def ^:dynamic *index* nil)
(def mock-connection (atom {:config {:name "test-backend"}}))
(def mock-config {:max-results 25})


(defn set-mock-config [f]
  (reset! config mock-config)
  (binding [*log-queries* false]
    (f)))


(defn get-test-messages []
  (let [cl (-> (Thread/currentThread) .getContextClassLoader)]
    (map (fn [[n filename]]
           (with-open [msg (.getResourceAsStream cl filename)]
             {:id {:group "test" :num (str n)}
              :content (.getBytes (slurp msg) "UTF-8")}))
         (sq/indexed ["test-message-1.mbox"]))))


(defn index-test-messages [f]
  (binding [*index* (RAMDirectory.)]
    (with-writer *index* iw
      (doseq [msg (map #(parse-message % mock-connection) (get-test-messages))]
        (index-message iw msg)))
    (f)))



(use-fixtures :once set-mock-config index-test-messages)

(deftest test-body-search
  (is (= (count (search *index* "\"You will need to define your own\""))
         1)
      "Phrase search on message body"))

(deftest test-from-search
  (is (= (count (search *index* "from:\"Mark Triggs\""))
         1)
      "From search"))

(deftest test-to-search-includes-cc
  (is (= (count (search *index* "to:\"Herman Toothrot\""))
         1)
      "To search includes CC"))

(deftest test-to-search-exclude-cc
  (is (= (count (search *index* "to:\"Herman Toothrot\" AND NOT cc:\"Herman Toothrot\""))
         0)
      "CC can be explicitly excluded"))

(deftest test-boolean-search
  (is (= (count (search *index* "from:\"Mark Triggs\" AND to:\"Mark Triggs\" AND \"shell script\""))
         1)
      "Boolean search"))

(deftest test-domain-search
  (is (= (count (search *index* "from:\"dishevelled.net\""))
         1)
      "Domain search"))

(deftest test-date-search
  (is (= (count (search *index* "date:20110806"))
         1)
      "Date search"))

(deftest test-range-search
  (is (= (count (search *index* "date:[20110806 TO 2012]"))
         1)
      "Date search"))




