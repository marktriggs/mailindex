(defproject mailindex "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.3.0-alpha4"]
                 [org.clojure.contrib/java-utils "1.3.0-alpha4"]
                 [org.clojure.contrib/seq "1.3.0-alpha4"]
                 [org.clojure.contrib/def "1.3.0-alpha4"]
                 [org.clojure.contrib/command-line "1.3.0-alpha4"]
                 [org.apache.lucene/lucene-core "3.0.3"]
                 [org.apache.lucene/lucene-highlighter "3.0.3"]
                 [javax.mail/mail "1.4.1"]
                 [org.apache.tika/tika-core "0.9"]
                 [org.apache.tika/tika-parsers "0.9"
                  :exclusions [edu.ucar/netcdf
                               commons-httpclient/commons-httpclient
                               org.apache.james/apache-mime4j
                               org.apache.commons/commons-compress
                               org.apache.pdfbox/pdfbox
                               org.bouncycastle/bcmail-jdk15
                               org.bouncycastle/bcprov-jdk15
                               org.apache.poi/poi
                               org.apache.poi/poi-scratchpad
                               org.apache.poi/poi-ooxml
                               org.apache.geronimo.specs/geronimo-stax-api_1.0_spec
                               asm/asm
                               com.drewnoakes/metadata-extractor
                               de.l3s.boilerpipe/boilerpipe]]]
  :dev-dependencies [[swank-clojure/swank-clojure "1.3.0-SNAPSHOT"]]
  :jar-exclusions [#"BCKEY.SF"]
  :warn-on-reflection true
  :main net.dishevelled.mailindex)
