(ns net.dishevelled.mailindex
  (:import (org.apache.lucene.index IndexReader IndexWriter
                                    IndexWriter$MaxFieldLength Term)
           (org.apache.lucene.search IndexSearcher BooleanQuery
                                     PhraseQuery BooleanClause$Occur TermQuery)
           (org.apache.lucene.document Document Field Field$Store
                                       Field$Index DateTools
                                       DateTools$Resolution)
           (org.apache.lucene.analysis SimpleAnalyzer)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.search.highlight QueryTermExtractor)
           (org.apache.lucene.store FSDirectory)
           (org.apache.lucene.queryParser QueryParser$Operator
                                          MultiFieldQueryParser)
           (java.net ServerSocket InetAddress BindException)
           (java.util Calendar Date SimpleTimeZone Vector)
           (java.text SimpleDateFormat)
           (java.io File PushbackReader))
  (:require [net.dishevelled.mailindex.fieldpool :as fieldpool])
  (:use clojure.contrib.duck-streams
        clojure.contrib.str-utils
        clojure.contrib.seq-utils
        clojure.contrib.def)
  (:gen-class
   :name MailIndex
   :main true))



(def *date-output-format*
     (doto (SimpleDateFormat. "EEE, dd MMM yyyy HH:mm:ss Z")
       (.setTimeZone (SimpleTimeZone. 0 "UTF"))))


(def *date-formatters*
     (map #(doto (SimpleDateFormat. %)
             (.setTimeZone (SimpleTimeZone. 0 "UTF")))
          ["EEE, dd MMM yyyy"
           "dd MMM yyyy"
           "E d MMM yyyy"
           "yyyy-MM-dd"
           "EEE, MMM d yyyy"
           "E, M d yyyy"]))


;;; Bound in (main)
(def *config* nil)


(declare parse-date)


(defvar *parse-rules*
  {
   "date"    {:pattern "date: "    :boost 10
              :preprocess #(when-let [date (parse-date %)]
                             (DateTools/dateToString
                              date
                              DateTools$Resolution/DAY))}

   "subject" {:pattern "subject: " :boost 5}
   "to"      {:pattern "to: "      :boost 2}
   "from"    {:pattern "from: "    :boost 3}
   "body"    {:pattern nil}
   "msgid"   {:pattern "message-id: "}
   })


;;; Utility functions

(defn get-field [#^Document doc field]
  "Return the first value for a given field from a Lucene document."
  (first (.getValues doc field)))


(defmacro chatty [msg & body]
  `(do (.print System/err (format "\r%-72.65s" ~msg))
       (do ~@body)))


;;; Message parsing

(defn parse-date
  "Turn `datestring' into a Date object by any means necessary!"
  [datestring]
  (some (fn [#^SimpleDateFormat formatter]
          (try
           (.parse formatter datestring)
           (catch Exception _)))
        *date-formatters*))


(defn load-body
  "Load the contents of the `body` seq into a Lucene `doc'.
Also adds fields for the line and character count of the message."
  [#^Document doc body line-count char-count]
  (if (seq body)
    (let [lines (take 50 body)
          s (str-join "\n" lines)]
      (do (.add doc (fieldpool/tokenized-unstored-field "body" s))
          (recur doc (drop 50 body)
                 (+ line-count (count lines))
                 (count s))))
    (doto doc
      (.add (fieldpool/stored-field "lines" (str line-count)))
      (.add (fieldpool/stored-field "chars" (str char-count))))))


(defn load-headers
  "Add interesting mail headers to Lucene `doc'."
  [#^Document doc lines]
  (doseq [#^String line (take-while #(not= % "") lines)]
    (when-let [[field value]
               (some (fn [[field opts]]
                       (when (and (:pattern opts)
                                  (.startsWith (.toLowerCase line)
                                               (:pattern opts)))
                         [field ((get opts :preprocess identity)
                                 (.substring line
                                             (inc (. line (indexOf " ")))))]))
                     *parse-rules*)]
      (when value
        (.add doc (fieldpool/tokenized-field field value)))))
  lines)


(defn parse-message
  "Produce a Lucene document from an email message."
  [msg connection]
  (let [#^Document doc (Document.)]

    (fieldpool/reset)

    (let [source (-> @connection :config :name)]
      (.add doc (fieldpool/stored-field "group" (-> msg :id :group)))
      (.add doc (fieldpool/stored-field "num" (-> msg :id :num)))
      (.add doc (fieldpool/stored-field "source" source))
      (.add doc (fieldpool/stored-field "id"
                                        (format "%s/%s@%s"
                                                (-> msg :id :group)
                                                (-> msg :id :num)
                                                source))))

    (load-body doc
               (load-headers doc (:lines msg))
               0 0)
    doc))



;;; Lucene indexing

(defn index-message
  "Index a message and add it to an IndexWriter."
  [#^IndexWriter writer msg]
  (.updateDocument writer (Term. "id" (get-field msg "id")) msg))


(defn index-age
  "Return the mtime of a Lucene index."
  [indexfile]
  (let [index (File. (str indexfile "/segments.gen"))
        index-mtime (.lastModified (File. (str indexfile "/segments.gen")))]
    (if (.exists index)
      index-mtime
      nil)))


(defmacro with-writer
  "Open a Lucene IndexWriter on `index' bound to `var' and evaluate `body'"
  [index var & body]
  `(do
     (IndexWriter/unlock (FSDirectory/getDirectory ~index))
     (with-open [#^IndexWriter ~var
                 (doto (IndexWriter.
                        ~index
                        (StandardAnalyzer.)
                        IndexWriter$MaxFieldLength/UNLIMITED)
                   (.setRAMBufferSizeMB 20)
                   (.setUseCompoundFile false))]
       ~@body)))


(defn do-deletes
  "Remove any deletes messages from `index'"
  [connection index]
  (with-open [reader #^IndexReader (IndexReader/open index)]
    (with-writer index writer
      (doseq [chunk (partition-all 1000 (range (.maxDoc reader)))]
        (let [doclist (into {}
                            (for [id chunk
                                  :when (not (.isDeleted reader id))
                                  :let [doc (.document reader id)]]
                              [{:group (get-field doc "group")
                                :num (get-field doc "num")}
                               (get-field doc "id")]))
              deletes ((:deleted-messages-fn @connection)
                       connection (keys doclist))]
          (doseq [d deletes]
            (.println System/err (str "Deleting from index: " (doclist d)))
            (.deleteDocuments writer (Term. "id" (doclist d)))))))))


(defn do-optimize
  "Optimize `index'"
  [index]
  (with-writer index iw
    (.optimize iw)))


(defn index
  "Adds `messages' to an index using IndexWriter `iw'."
  [connection messages iw & [cnt starttime]]
  (let [cnt (or cnt 0)
        starttime (or starttime (System/currentTimeMillis))]
    (when (seq messages)
      (try
       (when (zero? (mod cnt 1000))
         (.println System/err
                   (format "\nmsgs/sec: %.2f"
                           (float (/ cnt
                                     (inc (/ (- (System/currentTimeMillis)
                                                starttime)
                                             1000)))))))
       (chatty (format "[%d] Parsing message %s" (or cnt 0) (:id (first messages)))
               (index-message iw (parse-message (first messages) connection)))
       (recur connection (rest messages) iw [(inc (or cnt 0)) starttime])
       (catch Exception e
         (.println System/err (str "Agent got exception: " e))
         (.printStackTrace e))
       (catch Error e
         (.println System/err (str "Agent got error: " e))
         (.printStackTrace e))))))



(defn time-to-optimise?
  "True if the current time of day is a good time to optimise."
  []
  (let [hour (.. Calendar getInstance (get Calendar/HOUR_OF_DAY))]
    (= hour (:optimize-hour *config*))))


(defn start-indexing
  "Kick off the indexer."
  [state indexfile connections]
  (try
   (let [last-update (index-age indexfile)]
     (doseq [connection connections]
       (with-writer indexfile iw
         (index connection
                ((:new-messages-fn @connection) connection last-update)
                iw)))
     (when (time-to-optimise?)
       (doseq [connection connections]
         (do-deletes connection indexfile))
       (do-optimize indexfile)))
   (Thread/sleep (:reindex-frequency *config*))
   (catch Exception e
     (.println System/err e)
     (.printStackTrace e)))
  (send-off *agent* start-indexing indexfile connections))



;;; Query handling

(defn result-seq
  "Returns a lazy seq of a Lucene Hits object."
  [hits]
  (map #(let [doc (.doc hits %)]
          [(get-field doc "group")
           (get-field doc "num")
           (int (* (.score hits %) 100000))
           (concat
            [:date
             (when (get-field doc "date")
               (.format *date-output-format*
                        (DateTools/stringToDate (get-field doc "date"))))]
            (mapcat (fn [f] [(keyword f)
                             (get-field doc f)])
                    ["subject" "to" "from" "msgid" "lines" "chars"]))])
       (range (.length hits))))


(defn normalcase
  "Normalise the case of a query string."
  [s]
  (reduce (fn [#^String s #^String op]
            (.replaceAll s (str " " op " ") (str " " (. op toUpperCase) " ")))
          (.replaceAll (.toLowerCase s)
                       "\\[([0-9*]+) to ([0-9*]+)\\]"
                       "[$1 TO $2]")
          ["and" "or" "not"]))


(defn build-query
  "Construct a Lucene query from `querystr'."
  [querystr reader]
  (let [query (BooleanQuery.)
        all-fields (.parse (doto (MultiFieldQueryParser.
                                      (into-array (keys *parse-rules*))
                                      (StandardAnalyzer.))
                             (.setDefaultOperator QueryParser$Operator/AND))
                           (normalcase querystr))]
    (.setBoost all-fields 20)
    (.add query all-fields BooleanClause$Occur/MUST)

    (when-let [terms (seq (set (map #(.getTerm %)
                                    (QueryTermExtractor/getTerms
                                     (.rewrite query reader)))))]
      (doseq [field (keys *parse-rules*)]
        (let [boolean-or (BooleanQuery.)
              boolean-and (BooleanQuery.)]

          (doseq [term terms]
            (.add boolean-or
                  (TermQuery. (Term. field term))
                  BooleanClause$Occur/SHOULD)
            (.add boolean-and
                  (TermQuery. (Term. field term))
                  BooleanClause$Occur/MUST))

          (let [boost (or (:boost (get *parse-rules* field))
                          1)]
            (.setBoost boolean-or boost)
            (.setBoost boolean-and (* boost 10)))

          (.add query boolean-or BooleanClause$Occur/SHOULD)
          (.add query boolean-and BooleanClause$Occur/SHOULD))))
    (println query)
    query))


(defn search
  "Perform a search against `index' using `querystr'.  Returns the top
matching documents."
  [index querystr]
  (with-open [reader (IndexReader/open index)
              searcher (IndexSearcher. reader)]
    (doall (take (:max-results *config*)
                 (result-seq (.search searcher
                                      (build-query querystr reader)))))))


(defn handle-searches
  "Kick off the search handler."
  [state indexfile port]
  (try
   (.println System/err (format "Listening for searches on port %d" port))
   (with-open [server (ServerSocket. port 50 (InetAddress/getByName "127.0.0.1"))
               client (.accept server)
               in (reader (.getInputStream client))
               out (writer (.getOutputStream client))]
     (.println out (prn-str (search indexfile (.readLine in))))
     (.flush out))

   (catch BindException e
     (throw (RuntimeException. e)))
   (catch Exception e
     (.println System/err e)
     (.printStackTrace e)))
  (send-off *agent* handle-searches indexfile port))



;;; The main bit...

(defn -main [& args]
  (alter-var-root #'*config* (fn [_] (read (PushbackReader. (reader "config.clj")))))
  (let [{:keys [port indexfile]} *config*
        connections (map (fn [b]
                           (require (:backend b))
                           (let [conn (@(ns-resolve (:backend b)
                                                    'get-connection)
                                       b)]
                             (swap! conn assoc :config b)
                             conn))
                         (:backends *config*))
        searcher (agent nil)
        indexer (agent nil)]

    (send-off searcher handle-searches indexfile (Integer. port))
    (send-off indexer start-indexing indexfile connections)
    (await indexer searcher)))
