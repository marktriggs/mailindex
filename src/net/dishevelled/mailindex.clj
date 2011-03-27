(ns net.dishevelled.mailindex
  (:import (org.apache.lucene.index IndexReader IndexWriter
                                    IndexWriter$MaxFieldLength Term)
           (org.apache.lucene.search BooleanQuery TopDocs PhraseQuery BooleanClause$Occur TermQuery)
           (org.apache.lucene.document Document Field Field$Store
                                       Field$Index DateTools
                                       DateTools$Resolution)
           (org.apache.lucene.analysis SimpleAnalyzer)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.search.highlight QueryTermExtractor)
           (org.apache.lucene.store FSDirectory)
           (org.apache.lucene.util Version)
           (org.apache.lucene.queryParser QueryParser$Operator
                                          MultiFieldQueryParser)
           (java.net ServerSocket InetAddress BindException)
           (java.util Calendar Date SimpleTimeZone Vector Properties)
           (java.text SimpleDateFormat)
           (java.io File PushbackReader ByteArrayInputStream)

           (javax.mail.internet MimeMessage InternetAddress MimeMultipart)
           (javax.mail Session Part Message$RecipientType))
  (:require [net.dishevelled.mailindex.fieldpool :as fieldpool]
            [net.dishevelled.mailindex.searcher-manager :as searcher-manager])
  (:use clojure.java.io
        [clojure.string :only [join]]
        clojure.contrib.seq
        clojure.contrib.def)

  (:gen-class))


(def date-output-format
     (doto (SimpleDateFormat. "EEE, dd MMM yyyy HH:mm:ss Z")
       (.setTimeZone (SimpleTimeZone. 0 "UTF"))))


(def date-formatters
     (map #(doto (SimpleDateFormat. %)
             (.setTimeZone (SimpleTimeZone. 0 "UTF")))
          ["EEE, dd MMM yyyy"
           "dd MMM yyyy"
           "E d MMM yyyy"
           "yyyy-MM-dd"
           "EEE, MMM d yyyy"
           "E, M d yyyy"]))


;;; Set in (main)
(def config (atom nil))


(defn address-to-str [^InternetAddress address]
  (format "%s <%s>"
          (or (.getPersonal address) "")
          (.getAddress address)))


(defvar parse-rules
  {
   "date"    {:value-fn (fn [^MimeMessage msg]
                          (DateTools/dateToString
                           (or (.getSentDate msg)
                               (.getReceivedDate msg))
                           DateTools$Resolution/DAY))
              :boost 10}

   "subject" {:value-fn (fn [^MimeMessage msg] (.getSubject msg)) :boost 5}
   "to"      {:value-fn
              (fn [^MimeMessage msg] (join
                         " "
                         (map address-to-str
                              (.getRecipients msg Message$RecipientType/TO))))
              :boost 2}
   "from"    {:value-fn (fn [^MimeMessage msg] (address-to-str (first (.getFrom msg))))
              :boost 3}
   "body"    {}
   "msgid"   {:value-fn (fn [^MimeMessage msg] (.getMessageID msg))}
   })



;;; Utility functions

(defn error [fmt & args]
  (.println System/err (apply format fmt args)))

(defn get-field [^Document doc field]
  "Return the first value for a given field from a Lucene document."
  (first (.getValues doc field)))


(defmacro chatty [msg & body]
  `(do (.print System/err (format "\r%-72.65s" ~msg))
       (do ~@body)))


(defn count-lines [s]
  (reduce (fn [cnt ch]
            (if (= ch \newline)
              (inc cnt)
              cnt))
          0
          s))



;;; Message parsing

(declare parse-mime-part)

(defn parse-mime-multipart [^MimeMultipart multipart]
  (mapcat #(parse-mime-part (.getBodyPart multipart (int %)))
          (range (.getCount multipart))))


(defn strip-html [^String s]
  (let [content (org.apache.tika.sax.WriteOutContentHandler. 1000000)]
    (.parse (org.apache.tika.parser.html.HtmlParser.)
            (java.io.ByteArrayInputStream. (.getBytes s))
            content
            (org.apache.tika.metadata.Metadata.)
            (org.apache.tika.parser.ParseContext.))
    (.toString content)))


(defn parse-mime-part
  "Recursively parses a MIME part converting it into a sequence of strings."
  [^Part part]
  (try
    (let [content (.getContent part)]
      (condp re-find (.toLowerCase (.getContentType part))
        #"^text/(plain|calendar)" [(if (string? content)
                                     content
                                     (slurp content))]
        #"^text/(html|xml)" [(strip-html content)]
        #"^message/rfc822" (parse-mime-part content)
        #"^multipart/" (parse-mime-multipart (.getContent part))
        []))
    (catch java.io.UnsupportedEncodingException e
      [])))





(defn load-body
  "Load the contents of the `body` seq into a Lucene `doc'.
Also adds fields for the line and character count of the message."
  [^Document doc ^MimeMessage msg]

  (let [parts (parse-mime-part msg)]
    (doseq [part parts]
      (when part
        (.add doc (fieldpool/tokenized-unstored-field "body" part))))

    (doto doc
      (.add (fieldpool/stored-field "lines"
                                    (str (reduce + (map count-lines parts)))))
      (.add (fieldpool/stored-field "chars" (str (.getSize msg)))))))


(defn load-headers
  "Add interesting mail headers to Lucene `doc'."
  [^Document doc ^MimeMessage msg]
  (doseq [[field rule] parse-rules]
    (when (:value-fn rule)
      (when-let [value (try ((:value-fn rule) msg)
                            (catch Exception _
                              (error "Warning: missing value for field '%s'"
                                     field)
                              nil))]
        (.add doc (fieldpool/tokenized-field field value))))))


(defn parse-message
  "Produce a Lucene document from an email message."
  [msg connection]
  (let [^Document doc (Document.)]

    (fieldpool/reset)

    (let [source (-> @connection :config :name)]
      (.add doc (fieldpool/tokenized-field "group" (-> msg :id :group)))
      (.add doc (fieldpool/stored-field "num" (-> msg :id :num)))
      (.add doc (fieldpool/stored-field "source" source))
      (.add doc (fieldpool/stored-field "id"
                                        (format "%s/%s@%s"
                                                (-> msg :id :group)
                                                (-> msg :id :num)
                                                source))))

    (let [parsed-msg (MimeMessage. (Session/getDefaultInstance (Properties.))
                                   (ByteArrayInputStream. (:content msg)))]
      (load-headers doc parsed-msg)
      (load-body doc parsed-msg))
    doc))


;;; Lucene indexing

(defn index-message
  "Index a message and add it to an IndexWriter."
  [^IndexWriter writer msg]
  (.updateDocument writer (Term. "id" (get-field msg "id")) msg))


(defn index-age
  "Return the mtime of a Lucene index."
  [indexfile]
  (let [index (File. (str indexfile "/segments.gen"))
        index-mtime (.lastModified (file indexfile "segments.gen"))]
    (if (.exists index)
      index-mtime
      nil)))


(defmacro with-writer
  "Open a Lucene IndexWriter on `index' bound to `var' and evaluate `body'"
  [index var & body]
  `(do
     (let [dir# (FSDirectory/open (file ~index))]
       (IndexWriter/unlock dir#)
       (with-open [^IndexWriter ~var
                   (doto (IndexWriter.
                          dir#
                          (StandardAnalyzer. Version/LUCENE_30)
                          IndexWriter$MaxFieldLength/UNLIMITED)
                     (.setRAMBufferSizeMB 20)
                     (.setUseCompoundFile false))]
         ~@body))))


(defn do-deletes
  "Remove any deletes messages from `index'"
  [connection index]
  (with-open [reader ^IndexReader (IndexReader/open index)]
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
            (error "Deleting from index: %s" (doclist d))
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
          (error "\nmsgs/sec: %.2f"
                 (float (/ cnt
                            (inc (/ (- (System/currentTimeMillis)
                                       starttime)
                                    1000))))))
        (chatty (format "[%d] Parsing message %s" (or cnt 0) (:id (first messages)))
                (index-message iw (try (parse-message (first messages) connection)
                                       (catch Exception e
                                         (error "\nMessage failed to index: %s"
                                                (first messages))
                                         (throw e)))))
        (catch Throwable e
          (error "Agent got throwable: %s" e)
          (.printStackTrace e)))
      (recur connection (rest messages) iw [(inc (or cnt 0)) starttime]))))



(defn time-to-optimise?
  "True if the current time of day is a good time to optimise."
  []
  (let [hour (.. Calendar getInstance (get Calendar/HOUR_OF_DAY))]
    (= hour (:optimize-hour @config))))


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

    (searcher-manager/reopen indexfile)

    (Thread/sleep (:reindex-frequency @config))

    (send-off *agent* start-indexing indexfile connections)

    (catch Throwable e
      (error "%s" e)
      (.printStackTrace e))))



;;; Query handling

(defn result-seq
  "Returns a lazy seq of results from a TopDocs object."
  [^IndexReader ir ^TopDocs topdocs]
  (for [scoredoc (.scoreDocs topdocs)
        :let [doc (.document ir (.doc scoredoc))]]
    [(get-field doc "group")
     (get-field doc "num")
     (int (* (.score scoredoc) 100000))
     (concat
      [:date
       (when (get-field doc "date")
         (.format date-output-format
                  (DateTools/stringToDate (get-field doc "date"))))]
      (mapcat (fn [f] [(keyword f)
                       (get-field doc f)])
              (conj (keys parse-rules) "lines" "chars")))]))


(defn normalcase
  "Normalise the case of a query string."
  [s]
  (reduce (fn [^String s ^String op]
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
                                  Version/LUCENE_30
                                  (into-array (keys parse-rules))
                                  (StandardAnalyzer. Version/LUCENE_30))
                             (.setDefaultOperator QueryParser$Operator/AND))
                           (normalcase querystr))]
    (.setBoost all-fields 20)
    (.add query all-fields BooleanClause$Occur/MUST)

    (when-let [terms (seq (set (map #(.getTerm %)
                                    (QueryTermExtractor/getTerms
                                     (.rewrite query reader)))))]
      (doseq [field (keys parse-rules)]
        (let [boolean-or (BooleanQuery.)
              boolean-and (BooleanQuery.)]

          (doseq [term terms]
            (.add boolean-or
                  (TermQuery. (Term. field term))
                  BooleanClause$Occur/SHOULD)
            (.add boolean-and
                  (TermQuery. (Term. field term))
                  BooleanClause$Occur/MUST))

          (let [boost (or (:boost (get parse-rules field))
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
  [indexfile querystr]
  (let [searcher (searcher-manager/take indexfile)
        reader (.getIndexReader searcher)]
    (try (doall (result-seq reader
                            (.search searcher
                                     (build-query querystr reader)
                                     nil
                                     (:max-results @config))))
         (finally (searcher-manager/release indexfile searcher)))))


(defn handle-searches
  "Kick off the search handler."
  [state indexfile port]
  (try
   (error "Listening for searches on port %d" port)
   (with-open [server (ServerSocket. port 50 (InetAddress/getByName "127.0.0.1"))
               client (.accept server)
               in (reader (.getInputStream client))
               out (writer (.getOutputStream client))]
     (.write out (prn-str (search indexfile (.readLine in))))
     (.write out "\n")
     (.flush out))

   (catch BindException e
     (throw (RuntimeException. e)))
   (catch Throwable e
     (error "%s" e)
     (.printStackTrace e)))
  (send-off *agent* handle-searches indexfile port))



;;; The main bit...

(defn -main [& args]
  (reset! config (read (PushbackReader. (reader "config.clj"))))
  (let [{:keys [port indexfile]} @config
        connections (map (fn [b]
                           (require (:backend b))
                           (let [conn (@(ns-resolve (:backend b)
                                                    'get-connection)
                                       b)]
                             (swap! conn assoc :config b)
                             conn))
                         (:backends @config))
        searcher (agent nil)
        indexer (agent nil)]

    (send-off searcher handle-searches indexfile (Integer. port))
    (send-off indexer start-indexing indexfile connections)
    (await indexer searcher)))
