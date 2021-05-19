;;; Find hostnames and give us ngrams of the individual parts

(ns net.dishevelled.mailindex.MailindexURLFilterFactory
  (:import (org.apache.lucene.analysis TokenStream)
           (net.dishevelled.mailindex MailindexURLFilter))
  (:gen-class
   :extends org.apache.lucene.analysis.util.TokenFilterFactory
   :state state
   :init init
   :main false))

(defn -init [^java.util.Map args]
  [[args] {}])

(defn -create [this ^TokenStream token-stream]
  (MailindexURLFilter. token-stream))
