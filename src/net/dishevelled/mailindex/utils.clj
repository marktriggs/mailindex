(ns net.dishevelled.mailindex.utils
  (:require [clojure.java.io :refer [file]])
  (:import (org.apache.lucene.store Directory FSDirectory)))

(defn as-directory ^Directory [thing]
  (if (instance? Directory thing)
    thing
    (FSDirectory/open (.toPath (file thing)))))

(defn find-first
  "Returns the first item of coll for which (pred item) returns logical true.
  Consumes sequences up to the first match, will consume the entire sequence
  and return nil if no match is found."
  [pred coll]
  (first (filter pred coll)))
