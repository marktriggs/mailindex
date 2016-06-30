(ns net.dishevelled.mailindex.MailindexAnalyzer
  (:import (java.io IOException Reader)
           (org.apache.lucene.analysis.util CharArraySet StopwordAnalyzerBase WordlistLoader)
           (org.apache.lucene.analysis TokenStream Analyzer$TokenStreamComponents)
           (org.apache.lucene.analysis.standard ClassicTokenizer ClassicFilter)
           (org.apache.lucene.analysis.core StopAnalyzer LowerCaseFilter StopFilter)
           (net.dishevelled.mailindex MailindexURLFilter))
  (:gen-class
   :extends org.apache.lucene.analysis.util.StopwordAnalyzerBase
   :exposes-methods {createComponents parentCreateComponents}
   :init init
   :main false))


(def stopwords StopAnalyzer/ENGLISH_STOP_WORDS_SET)

(defn -init
  ([] [[stopwords] {}]))

(defn -createComponents [this ^String fieldName]
  (let [src (doto (ClassicTokenizer.)
              (.setMaxTokenLength 255))
        tokenizer (-> (ClassicFilter. src)
                      (LowerCaseFilter.)
                      (StopFilter. stopwords)
                      (MailindexURLFilter.))]
    (Analyzer$TokenStreamComponents. src tokenizer)))


;;   @Override
;;   protected TokenStreamComponents createComponents(final String fieldName) {
;;     final ClassicTokenizer src = new ClassicTokenizer();
;;     src.setMaxTokenLength(maxTokenLength);
;;     TokenStream tok = new ClassicFilter(src);
;;     tok = new LowerCaseFilter(tok);
;;     tok = new StopFilter(tok, stopwords);
;;     return new TokenStreamComponents(src, tok) {
;;       @Override
;;       protected void setReader(final Reader reader) {
;;         src.setMaxTokenLength(ClassicAnalyzer.this.maxTokenLength);
;;         super.setReader(reader);
;;       }
;;     };
;;   }
;; }
