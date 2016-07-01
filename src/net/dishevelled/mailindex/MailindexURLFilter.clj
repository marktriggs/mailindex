;;; Find hostnames and give us ngrams of the individual parts

(ns net.dishevelled.mailindex.MailindexURLFilter
  (:import (org.apache.lucene.analysis TokenStream)
           (org.apache.lucene.analysis.tokenattributes CharTermAttribute
                                                       TypeAttribute
                                                       PositionIncrementAttribute)
           (java.util LinkedList)
           (net.dishevelled.mailindex MailindexURLFilter))
  (:gen-class
   :exposes {input {:get input}}
   :extends org.apache.lucene.analysis.TokenFilter
   :state state
   :init init
   :post-init postInit
   :main false))

(def char-term-attribute (atom nil))
(def type-attribute (atom nil))
(def pending-tokens ())

(defn -init [^TokenStream token-stream]
  [[token-stream] (atom {:char-term-attribute nil
                         :type-attribute nil
                         :position-attribute nil
                         :pending-tokens (LinkedList.)})])

(defn -postInit [^MailindexURLFilter this & _]
  (swap! (.state this)
         (fn [state]
           (-> state
               (assoc :char-term-attribute (.addAttribute this CharTermAttribute))
               (assoc :type-attribute (.addAttribute this TypeAttribute))
               (assoc :position-attribute (.addAttribute this PositionIncrementAttribute))))))

(defn char-term-attribute ^CharTermAttribute [^MailindexURLFilter this]
  (:char-term-attribute @(.state this)))

(defn type-attribute ^TypeAttribute [^MailindexURLFilter this]
  (:type-attribute @(.state this)))

(defn position-attribute ^PositionIncrementAttribute [^MailindexURLFilter this]
  (:position-attribute @(.state this)))

(defn pending-tokens ^LinkedList [^MailindexURLFilter this]
  (:pending-tokens @(.state this)))

(defn hostname-permutations [^String s]
  (let [parts (.split s "\\.")]
   (reductions (fn [state n] (clojure.string/join "." (drop n parts)))
               s
               (range 0 (dec (count parts))))))

(defn add-url-tokens [^MailindexURLFilter this]
  (let [hostname (str (char-term-attribute this))]
    (let [parts (.split hostname "\\.")]
      (doseq [domain (concat (hostname-permutations hostname)
                             parts)]
        (.push (pending-tokens this)
               domain)))))

(defn add-email-tokens [^MailindexURLFilter this]
  (let [email (str (char-term-attribute this))]
    (let [[username domain] (.split email "@")]
      (doseq [domain (concat (hostname-permutations domain)
                             [username domain])]
        (.push (pending-tokens this)
               domain)))))

(defn emit-pending-token [^MailindexURLFilter this position-increment]
  (let [token (.pop (pending-tokens this))
        char-term-attribute (char-term-attribute this)
        position-attribute (position-attribute this)]
    (.setEmpty char-term-attribute)
    (.append char-term-attribute ^String token)
    (.setType (type-attribute this) "<ALPHANUM>")
    (.setPositionIncrement position-attribute (int position-increment)))
  true)

(defn -incrementToken [^MailindexURLFilter this]
  (cond (not (empty? (pending-tokens this))) (emit-pending-token this 0)
        (-> this .input .incrementToken)
        (cond (= (.type (type-attribute this)) "<HOST>")
              (do (add-url-tokens this)
                  (emit-pending-token this 1))
              (= (.type (type-attribute this)) "<EMAIL>")
              (do (add-email-tokens this)
                  (emit-pending-token this 1))
              :else true)
        :else false))
