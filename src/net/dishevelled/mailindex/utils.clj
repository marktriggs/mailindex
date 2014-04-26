(ns net.dishevelled.mailindex.utils
  (:import (org.apache.lucene.store Directory)
           (org.apache.lucene.store FSDirectory))
  (:use clojure.java.io))

(defn as-directory [thing]
  (if (instance? Directory thing)
    thing
    (FSDirectory/open (file thing))))

(defn find-first
  "Returns the first item of coll for which (pred item) returns logical true.
  Consumes sequences up to the first match, will consume the entire sequence
  and return nil if no match is found."
  [pred coll]
  (first (filter pred coll)))
