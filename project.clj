(defproject mailindex "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.2.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.2.0-master-SNAPSHOT"]
		 [org.apache.lucene/lucene-core "2.9.2"]
		 [org.apache.lucene/lucene-highlighter "2.9.2"]]
  :dev-dependencies [[leiningen/lein-swank "1.1.0"]] 
  :namespaces :all
  :main MailIndex)
