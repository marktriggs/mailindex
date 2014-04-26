(ns net.dishevelled.test.mailindex
  (:require [clojure.java.io :refer :all]
            [clojure.test :refer :all]
            [net.dishevelled.mailindex :refer :all])
  (:import (org.apache.lucene.store RAMDirectory)))

(defn indexed
  "Returns a lazy sequence of [index, item] pairs, where items come
  from 's' and indexes count up from zero.

  (indexed '(a b c d))  =>  ([0 a] [1 b] [2 c] [3 d])"
  [s]
  (map vector (iterate inc 0) s))

(def ^:dynamic *index* nil)
(def mock-connection (atom {:config {:name "test-backend"}}))
(def mock-config {:max-results 25})


(defn set-mock-config [f]
  (reset! config mock-config)
  (binding [*log-queries* false]
    (f)))


(defn get-test-messages []
  (map (fn [[n filename]]
         {:id {:group "test" :num (str n)}
          :content (.getBytes (slurp (resource filename)) "UTF-8")})
       (indexed ["test-message-1.mbox"
                    "test-message-with-html-attachment.mbox"])))


(defn index-test-messages [f]
  (binding [*index* (RAMDirectory.)]
    (with-writer *index* iw
      (doseq [msg (map #(parse-message % mock-connection) (get-test-messages))]
        (index-message iw msg)))
    (f)))



(use-fixtures :once set-mock-config index-test-messages)

(deftest test-body-search
  (is (> (count (search *index* "\"You will need to define your own\""))
         0)
      "Phrase search on message body"))

(deftest test-from-search
  (is (> (count (search *index* "from:\"Mark Triggs\""))
         0)
      "From search"))

(deftest test-to-search-includes-cc
  (is (> (count (search *index* "to:\"Herman Toothrot\""))
         0)
      "To search includes CC"))

(deftest test-to-search-exclude-cc
  (is (= (count (search *index* "to:\"Herman Toothrot\" AND NOT cc:\"Herman Toothrot\""))
         0)
      "CC can be explicitly excluded"))

(deftest test-boolean-search
  (is (> (count (search *index* "from:\"Mark Triggs\" AND to:\"Mark Triggs\" AND \"shell script\""))
         0)
      "Boolean search"))

(deftest test-domain-search
  (is (> (count (search *index* "from:\"dishevelled.net\""))
         0)
      "Domain search"))

(deftest test-date-search
  (is (> (count (search *index* "date:20110806"))
         0)
      "Date search"))

(deftest test-range-search
  (is (> (count (search *index* "date:[20110806 TO 2012]"))
         0)
      "Date search"))

(deftest test-html-attachment
  (is (> (count (search *index* "\"can't believe it works\""))
         0)
      "HTML attachment"))




