(ns net.dishevelled.mailindex
  (:require [clojure.java.io :refer [file reader writer]]
            [clojure.string :refer [join]]
            [clojure.tools.cli :refer [parse-opts]]
            [net.dishevelled.mailindex.fieldpool :as fieldpool]
            [net.dishevelled.mailindex.searcher-manager :as searcher-manager]
            [net.dishevelled.mailindex.utils :as utils])
  (:import (net.dishevelled.mailindex MailindexAnalyzer)
           (java.io ByteArrayInputStream File PushbackReader)
           (java.net BindException InetAddress ServerSocket)
           (java.text SimpleDateFormat)
           (java.util Calendar Date Properties SimpleTimeZone)
           (javax.mail Message$RecipientType Part Session)
           (javax.mail.internet InternetAddress MimeMessage
                                MimeMultipart)
           (org.apache.lucene.analysis.standard ClassicAnalyzer)
           (org.apache.lucene.document DateTools DateTools$Resolution
                                       Document)
           (org.apache.lucene.store Directory)
           (org.apache.lucene.index DirectoryReader IndexReader IndexWriter
                                    IndexWriterConfig MultiFields Term)
           (org.apache.lucene.queryparser.classic MultiFieldQueryParser
                                                  QueryParser$Operator)
           (org.apache.lucene.search IndexSearcher BooleanClause$Occur
                                     BooleanClause BoostQuery BooleanQuery
                                     BooleanQuery$Builder PhraseQuery
                                     PhraseQuery$Builder TermQuery TopDocs
                                     ScoreDoc Sort SortField SortField$Type)
           (org.apache.lucene.search.highlight QueryTermExtractor WeightedTerm)
           (org.apache.lucene.util Bits Version))
  (:gen-class))

(def MAX_PART_BYTES (* 20 1024 1024))

(def ^SimpleDateFormat date-output-format
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

(def ^:dynamic *log-queries* true)


;;; Set in (main)
(def config (atom nil))


(def debug-out (atom nil))

(defn debug [fmt & args]
  (when (:debug-log @config)
    (when-not @debug-out
      (swap! debug-out #(or % (writer (:debug-log @config))))
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread.
                         (fn []
                           (try (.close ^java.io.Writer @debug-out)
                                (catch Exception _))))))
    (.write ^java.io.Writer @debug-out
            (str (Date.)
                 "\t"
                 (apply format fmt args)
                 "\n"))
    (.flush ^java.io.Writer @debug-out)))


(defn address-to-str [^InternetAddress address]
  (format "%s <%s>"
          (or (.getPersonal address) "")
          (.getAddress address)))


(defn address-to-tokens [^InternetAddress address]
  (.replace (str (.getAddress address))
            "@" " "))


(def parse-rules
  {
   "date" {:value-fn (fn [^MimeMessage msg]
                       (DateTools/dateToString
                        (or (.getSentDate msg)
                            (.getReceivedDate msg))
                        DateTools$Resolution/DAY))
           :boost 10
           :for-sorting true}

   "subject" {:value-fn (fn [^MimeMessage msg] (.getSubject msg)) :boost 5}

   "to" {:value-fn
         (fn [^MimeMessage msg] (join
                                 " "
                                 (map address-to-str
                                      (.getRecipients msg Message$RecipientType/TO))))
         :boost 2
         :linked-fields ["to_tokens" "cc"]}

   "cc" {:value-fn
         (fn [^MimeMessage msg] (join
                                 " "
                                 (map address-to-str
                                      (.getRecipients msg Message$RecipientType/CC))))
         :boost 2
         :linked-fields ["to_tokens" "cc"]}

   "to_tokens" {:value-fn (fn [^MimeMessage msg]
                            (join " "
                                  (concat (map address-to-tokens
                                               (.getRecipients
                                                msg
                                                Message$RecipientType/TO))
                                          (map address-to-tokens
                                               (.getRecipients
                                                msg
                                                Message$RecipientType/CC)))))
                :boost 1}

   "from" {:value-fn (fn [^MimeMessage msg] (address-to-str (first (.getFrom msg))))
           :boost 3
           :linked-fields ["from_tokens"]}

   "from_tokens" {:value-fn (fn [^MimeMessage msg]
                              (address-to-tokens
                               (first (.getFrom msg))))
                  :boost 2}

   "body" {}

   "msgid" {:value-fn (fn [^MimeMessage msg] (.getMessageID msg))}
   })



;;; Utility functions

(defn error [fmt & args]
  (.println System/err (apply format fmt args)))

(defn get-field ^String [^Document doc field]
  "Return the first value for a given field from a Lucene document."
  (first (.getValues doc field)))


(defmacro chatty [msg & body]
  `(do (.print System/err (format "\r%-72.65s" ~msg))
       (do ~@body)))


(defn count-lines [^String s]
  (loop [i (dec (count s))
         result 0]
    (if (>= i 0)
      (recur
       (dec i)
       (if (= (.charAt s i) \newline)
         (inc result)
         result))
      result)))


;;; Message parsing

(declare parse-mime-part)

(defn parse-mime-multipart [^MimeMultipart multipart]
  (mapcat #(parse-mime-part (.getBodyPart multipart (int %)))
           (range (.getCount multipart))))


(defn strip-html [^String s]
  (let [content (org.apache.tika.sax.WriteOutContentHandler. (int (* MAX_PART_BYTES 6)))]
    (.parse (org.apache.tika.parser.html.HtmlParser.)
            (java.io.ByteArrayInputStream. (.getBytes s))
            content
            (org.apache.tika.metadata.Metadata.)
            (org.apache.tika.parser.ParseContext.))
    (.toString content)))

(defn extract-part-metadata [^Part part]
  (remove nil? [(.getContentType part) (.getFileName part)]))

(defn parse-mime-part
  "Recursively parses a MIME part converting it into a sequence of strings."
  [^Part part]
  (concat
   (extract-part-metadata part)
   (try
     (if (and (> (.getSize part) MAX_PART_BYTES)
              (not (re-find #"^multipart/" (.toLowerCase (.getContentType part)))))
       (do (println (format "Skipping giant part: (type: %s) %s"
                            (.toLowerCase (.getContentType part))
                            (.getSize part)))
           [])
       (let [content (.getContent part)]
         (condp re-find (.toLowerCase (.getContentType part))
           #"^text/(plain|calendar)" [(if (string? content)
                                        content
                                        (slurp content))]
                #"^text/(html|xml)" [(strip-html content)]
                #"^message/rfc822" (parse-mime-part content)
                #"^multipart/" (parse-mime-multipart (.getContent part))
                [])))
     (catch java.io.UnsupportedEncodingException e
       []))))





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
                            (catch Exception e
                              (error "Warning: missing value for field '%s': %s"
                                     field e)
                              nil))]
        (if (:for-sorting rule)
          (do (.add doc (fieldpool/stored-field field value))
              (.add doc (fieldpool/sort-field (str field "-sort") value)))
          (.add doc (fieldpool/tokenized-field field value)))))))


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
  [^String indexfile]
  (let [stat-file (File. (str indexfile "/mailindex.stat"))]
    (if (.exists stat-file)
      (Long/valueOf ^String (slurp stat-file))
      nil)))

(defn store-index-age [^String indexfile ^Date date]
  "Record the mtime of a Lucene index."
  (let [stat-file (File. (str indexfile "/mailindex.stat"))]
    (spit stat-file (str (.getTime date)))))



(defmacro with-writer
  "Open a Lucene IndexWriter on `index' bound to `var' and evaluate `body'"
  [index var & body]
  `(do
     (let [dir# (utils/as-directory ~index)]
       (with-open [^IndexWriter ~var
                   (doto (IndexWriter.
                          dir#
                          (doto (IndexWriterConfig. (doto (MailindexAnalyzer.)
                                                      (.setVersion Version/LUCENE_6_1_0)))
                            (.setUseCompoundFile false))))]
         ~@body))))


(defn do-deletes
  "Remove any deleted messages from `index'"
  [connection indexfile]
  (debug "Starting a deletes run now")
  (with-open [reader ^IndexReader (DirectoryReader/open
                                   (utils/as-directory indexfile))]
    (let [live-docs ^Bits (MultiFields/getLiveDocs reader)]
      (with-writer indexfile writer
        (doseq [chunk (partition-all 1000 (range (.maxDoc reader)))]
          (let [doclist (into {}
                              (for [id chunk
                                    :when (or (not live-docs) (.get live-docs id))
                                    :let [doc (.document reader id)]]
                                [{:group (get-field doc "group")
                                  :num (get-field doc "num")}
                                 (get-field doc "id")]))
                deletes ((:deleted-messages-fn @connection)
                         connection (keys doclist))]
            (doseq [d deletes]
              (debug "Deleting from index: %s" (doclist d))
              (.deleteDocuments writer
                                ^"[Lorg.apache.lucene.index.Term;" (into-array [(Term. "id" ^String (doclist d))])))))))))


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
    (let [last-update (index-age indexfile)
          round-start (java.util.Date.)]
      (doseq [connection connections]
        (with-writer indexfile iw
          (index connection
                 ((:new-messages-fn @connection) connection last-update)
                 iw))
        (store-index-age indexfile round-start))
      (when (time-to-optimise?)
        (doseq [connection connections]
          (do-deletes connection indexfile))))

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
  (for [^ScoreDoc scoredoc (.scoreDocs topdocs)
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
  [^String s]
  (reduce (fn [^String s ^String op]
            (.replaceAll s (str " " op " ") (str " " (. op toUpperCase) " ")))
          (.replaceAll (.toLowerCase s)
                       "\\[([0-9*]+) to ([0-9*]+)\\]"
                       "[$1 TO $2]")
          ["and" "or" "not"]))


(defn expand-query [query]
  (cond (instance? BooleanQuery query)
        (let [new-query (BooleanQuery$Builder.)]
          (doseq [^BooleanClause clause (.clauses ^BooleanQuery query)]
            (.add new-query
                  (expand-query (.getQuery clause))
                  (.getOccur clause)))
          (.build new-query))

        (and (instance? PhraseQuery query))
        (if-let [linked-fields (:linked-fields
                                (parse-rules (.field
                                              ^Term (first (.getTerms ^PhraseQuery query)))))]
          (let [new-query (BooleanQuery$Builder.)]
            (.add new-query query BooleanClause$Occur/SHOULD)
            (doseq [^String field linked-fields]
              (let [phrase (PhraseQuery$Builder.)]
                (doseq [^Term term (.getTerms ^PhraseQuery query)]
                  (.add phrase (Term. field (.text term))))
                (.add new-query (.build phrase) BooleanClause$Occur/SHOULD)))
            (.build new-query))
          query)

        (instance? TermQuery query)
        (let [term (.getTerm ^TermQuery query)]
          (if-let [linked-fields (:linked-fields (parse-rules (.field term)))]
            (let [new-query (BooleanQuery$Builder.)]
              (.add new-query query BooleanClause$Occur/SHOULD)

              (doseq [^String field linked-fields]
                (.add new-query (TermQuery. (Term. field (.text term)))
                      BooleanClause$Occur/SHOULD))

              (.build new-query))
            query))

        :else query))


(defn build-query
  "Construct a Lucene query from `querystr'."
  [querystr reader]
  (let [query (BooleanQuery$Builder.)
        search-fields (map first (filter (fn [[k v]] (not (:for-sorting v))) parse-rules))
        all-fields (expand-query
                    (.rewrite (.parse (doto (MultiFieldQueryParser.
                                             (into-array search-fields)
                                             (ClassicAnalyzer.))
                                        (.setDefaultOperator QueryParser$Operator/AND))
                                      (normalcase querystr))
                              reader))]
    (.add query (BoostQuery. all-fields 20) BooleanClause$Occur/MUST)

    (when-let [terms (seq (set (map #(.getTerm ^WeightedTerm %)
                                     (QueryTermExtractor/getTerms
                                      (.rewrite (.build query) reader)))))]
      (doseq [^String field (keys parse-rules)]
        (let [boolean-or (BooleanQuery$Builder.)
              boolean-and (BooleanQuery$Builder.)]

          (doseq [^String term terms]
            (.add boolean-or
                  (TermQuery. (Term. field term))
                  BooleanClause$Occur/SHOULD)
            (.add boolean-and
                  (TermQuery. (Term. field term))
                  BooleanClause$Occur/MUST))

          (let [boost (or (:boost (get parse-rules field))
                          1)]
            (.add query (BoostQuery. (.build boolean-or) boost) BooleanClause$Occur/SHOULD)
            (.add query (BoostQuery. (.build boolean-and) (* boost 10)) BooleanClause$Occur/SHOULD)))))
    (when *log-queries*
      (println (.build query)))
    (.build query)))


(defn extract-query-options [input]
  (let [option-fragments (re-seq #"\{([^ ]+)\}" input)
        querystr (reduce (fn [s [option _]] (.replace ^String s ^String option ""))
                         input
                         option-fragments)]
    [querystr
     (into {} (map (fn [[_ option]] (vec (.split ^String option ":" 2)))
                   option-fragments))]))


(defn search
  "Perform a search against `index' using `querystr'.  Returns the top
  matching documents."
  [indexfile input]
  (let [^IndexSearcher searcher (searcher-manager/take indexfile)
        reader (.getIndexReader searcher)
        [querystr options] (extract-query-options input)]
    (try (doall (result-seq reader
                            (.search searcher
                                     (build-query querystr reader)
                                     (:max-results @config)
                                     (if (= (options "sort") "date")
                                       (Sort. (SortField. "date-sort" SortField$Type/STRING true))
                                       Sort/RELEVANCE))))
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
      (.write out (prn-str (search indexfile (.readLine ^java.io.BufferedReader in))))
      (.write out "\n")
      (.flush out))

    (catch BindException e
      (throw (RuntimeException. e)))
    (catch Throwable e
      (error "%s" e)
      (.printStackTrace e)))
  (send-off *agent* handle-searches indexfile port))



;;; The main bit...

(def cli-options
  [["-c" "--config-file FILE" "Configuration file."
    :default "config.clj"]
   ["-h" "--help" "Display this help message."]])

(defn usage [options-summary]
  (->> ["Usage: [--config-file config.clj]"
        "Options:"
        options-summary]
       (join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]

    ;; Handle help and error conditions
    (cond
      (:help options) (exit 0 (usage summary))
      (not= (count arguments) 0) (exit 1 (usage summary))
      errors (exit 1 (error-msg errors)))

    ;; Execute program with options
    (reset! config (read (PushbackReader. (reader (:config-file options)))))
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

      (send-off searcher handle-searches indexfile (Integer/valueOf (long port)))
      (send-off indexer start-indexing indexfile connections)
      (await indexer searcher))))


