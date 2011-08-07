(ns net.dishevelled.mailindex.utils
  (:import (org.apache.lucene.store Directory)
           (org.apache.lucene.store FSDirectory))
  (:use clojure.java.io))

(defn as-directory [thing]
  (if (instance? Directory thing)
    thing
    (FSDirectory/open (file thing))))
