(defproject mailindex "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.apache.lucene/lucene-core "3.0.3"]
                 [org.apache.lucene/lucene-highlighter "3.0.3"]
                 [javax.mail/mail "1.4.7"]
                 [org.apache.tika/tika-core "1.5"]
                 [org.apache.tika/tika-parsers "1.5"
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
  :profiles {:dev {:resource-paths ["test/resources"]}}
  :jar-exclusions [#"BCKEY.SF"]
  :global-vars {*warn-on-reflection* true}
  :aot [net.dishevelled.mailindex]
  :main net.dishevelled.mailindex)
