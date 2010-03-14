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
           (java.net ServerSocket)
           (java.util Calendar Date SimpleTimeZone Vector)
           (java.text SimpleDateFormat)
           (java.io File))
  (:refer-clojure :exclude [line-seq])
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


(defvar *index-delay* 60000
  "The number of milliseconds between indexing rounds.")

(defvar *server-port* 4321
  "The port we listen on for search requests")


(defvar *hit-count* 1000
  "The number of hits to return when searching.")


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


(defvar *max-size* (* 10 1024 1024)
  "The maximum size (in bytes) that a message can be before being ignored.")


(defvar *optimise-hour* 4
  "The hour of the day we're allowed to optimise")


;;; Utility functions

(defn get-field [#^Document doc field]
  "Return the first value for a given field from a Lucene document."
  (first (.getValues doc field)))



;;; Message parsing

(defn parse-date [datestring]
  (some (fn [#^SimpleDateFormat formatter]
	  (try
	   (.parse formatter datestring)
	   (catch Exception _)))
	*date-formatters*))


(defn process-date [datestring]
  "Convert a date string from a message into something we can index."
  (when-let [date (parse-date datestring)]
    (DateTools/dateToString date DateTools$Resolution/DAY)))


(defn load-body [#^Document doc body
		 line-count char-count]
  (if (seq body)
    (let [lines (take 100 body)
	  s (str-join "\n" lines)]
      (do (.add doc (fieldpool/tokenized-unstored-field "body" s))
	  (recur doc (drop 100 lines)
		 (+ line-count (count lines))
		 (count s))))
    (doto doc
      (.add (fieldpool/stored-field "lines" (str line-count)))
      (.add (fieldpool/stored-field "chars" (str char-count))))))


(defn load-headers [#^Document doc headers]
  "Add any interesting mail headers to our Lucene document."
  (doseq [#^String line headers]
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
  doc)


;;; Had a few problems with line-seq from clojure.core.  This variant is fully-lazy.
(defn line-seq [#^java.io.BufferedReader rdr]
  (lazy-seq
    (when-let [line (.readLine rdr)]
      (cons line (line-seq rdr)))))


(defn parse-message [#^String filename basedir]
  "Produce a Lucene document from an mbox file containing a single message."
  (let [#^Document doc (Document.)
        [whole group num]
        (first
         (re-seq (re-pattern (str basedir "/*" "(.*?)" "/" "([0-9]+)$"))
                 filename))]

    (fieldpool/reset)

    (.add doc (fieldpool/stored-field "filename" filename))
    (.add doc (fieldpool/stored-field "num" num))
    (.add doc (fieldpool/stored-field "group" group))
    (.add doc (fieldpool/stored-field "id" (format "%s@%s" num group)))

    (with-open [#^java.io.BufferedReader rdr (reader (File. filename))]
      (load-headers doc (take-while #(not= % "") (line-seq rdr)))
      (load-body doc (line-seq rdr) 0 0))))


(defn index-message [#^IndexWriter writer msg]
  "Index a message and add it to an IndexWriter."
  (.updateDocument writer (Term. "id" (get-field msg "id")) msg))


(defn index-age [indexfile]
  "Return the mtime of an index."
  (let [index (File. (str indexfile "/segments.gen"))
        index-mtime (.lastModified (File. (str indexfile "/segments.gen")))
        now (.getTime (Date.))]
    (if (.exists index)
      (inc (int (/ (- now index-mtime) 1000 60 60 24)))
      nil)))



(defn find-updates [basedir offset]
  "Find any messages that have been updated in the last `offset' days."
  (let [cmd ["find" basedir "-type" "f"]
        cmd (if offset (concat cmd ["-mtime" (str "-" offset)]) cmd)]
    (set (filter #(re-find #"/[0-9]+$" %)
                 (line-seq (reader (.. Runtime
                                       getRuntime
                                       (exec (into-array cmd))
                                       getInputStream)))))))


(defmacro chatty [msg & body]
  `(do (.print System/err (format "\r%-72.65s" ~msg))
       (do ~@body)))


(defn find-deletes [index writer]
  "Scan our index for any messages that no longer exist."
  (with-open [reader #^IndexReader (IndexReader/open index)]
      (dotimes [docnum (.maxDoc reader)]
          (when (not (.isDeleted reader docnum))
            (let [doc (.document reader docnum)
                  filename (get-field doc "filename")]
              (when (not (.exists (File. filename)))
                (.println System/err (str "Removing from index: " filename))
                (.deleteDocuments writer (Term. "filename" filename))))))))


(defn format-status [percent-complete mps filename]
  (format "(%.2f%% %s msgs/s) Indexing %s"
          (float (* percent-complete
                    100))
          (if mps
            (format "%.2f" (float mps))
            "?")
          filename))


(defn index [basedir index optimise seen-messages]
  "Adds any modified messages from `basedir' to `index'.
If optimise is true, optimise the index once this is done.
Any paths contained in `seen-messages' are skipped."
  (try
   (IndexWriter/unlock (FSDirectory/getDirectory index))
   (let [updates (find-updates basedir (index-age index))
         to-index (clojure.set/difference
                   updates
                   seen-messages)]
     (with-open [#^IndexWriter writer (doto (IndexWriter.
                                             index
                                             (StandardAnalyzer.)
                                             IndexWriter$MaxFieldLength/UNLIMITED)
                                        (.setRAMBufferSizeMB 20)
                                        (.setUseCompoundFile false))]

       (loop [[fileset & fss] (partition-all 1000 to-index)
              msgcount 0
              mps nil]
         (let [start (System/currentTimeMillis)]
           (doseq [#^String filename fileset]
             (chatty (format-status (/ msgcount (count to-index))
                                    mps
                                    filename)
	       (try (index-message writer (parse-message filename basedir))
		    (catch java.io.FileNotFoundException e
		      (.println System/err "Oops.  Lost it")))))
           (when (seq fss)
             (recur fss
                    (+ msgcount (count fileset))
                    (/ (count fileset)
                       (/ (- (System/currentTimeMillis) start)
                          1000))))))

       (when optimise
         (chatty "Optimising"
	   (find-deletes index writer)
	   (.optimize writer true)))
       (clojure.set/union updates seen-messages)))
   (catch Exception e
     (.println System/err (str "Agent got exception: " e))
     seen-messages)))


;;; Query handling
(defn result-seq [hits]
  "Returns a lazy seq of a Lucene Hits object."
  (map #(let [doc (.doc hits %)]
          [(get-field doc "group")
           (get-field doc "num")
           (int (* (.score hits %) 100000))
	   (concat [:date
		    (when (get-field doc "date")
		      (.format *date-output-format*
			       (DateTools/stringToDate (get-field doc "date"))))]
		   (mapcat (fn [f] [(keyword f)
				    (get-field doc f)])
			   ["subject" "to" "from" "msgid" "lines" "chars"]))])
       (range (.length hits))))


(defn normalcase [s]
  "Normalise the case of a query string."
  (reduce (fn [#^String s #^String op]
            (.replaceAll s (str " " op " ") (str " " (. op toUpperCase) " ")))
          (.replaceAll (.toLowerCase s)
                       "\\[([0-9*]+) to ([0-9*]+)\\]"
                       "[$1 TO $2]")
          ["and" "or" "not"]))


(defn build-query [querystr reader]
  "Construct a Lucene query from `querystr'."
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


(defn search [index querystr]
  "Perform a search against `index' using `querystr'.  Returns the top
matching documents."
  (with-open [reader (IndexReader/open index)
              searcher (IndexSearcher. reader)]
    (doall (take *hit-count*
                 (result-seq (.search searcher
                                      (build-query querystr reader)))))))


(defn time-to-optimise? []
  "True if the current time of day is a good time to optimise."
  (let [hour (.. Calendar getInstance (get Calendar/HOUR_OF_DAY))]
    (= hour *optimise-hour*)))


(defn start-indexing
  "Kick off the indexer."
  ([state maildir indexfile]
     (start-indexing state maildir indexfile #{}))
  ([state maildir indexfile seen-messages]
     (let [new-seen (index maildir
                           indexfile
                           (time-to-optimise?)
                           seen-messages)]
       (Thread/sleep *index-delay*)
       (send-off *agent* start-indexing maildir indexfile new-seen))))



(defn handle-searches [state indexfile port]
  "Kick off the search handler."
  (try
   (with-open [server (ServerSocket. port)
               client (.accept server)
               in (reader (.getInputStream client))
               out (writer (.getOutputStream client))]
       (.println out (prn-str (search indexfile (.readLine in))))
     (.flush out))
   (catch Exception e
     (.println System/err e)
     (.printStackTrace e)))
  (send-off *agent* handle-searches indexfile port))


(defn -main [& args]
  (if (empty? args)
    (println "Usage: mailindex.clj <mail directory> <index file> <listen port>")
    (let [[maildir indexfile port] args
	  indexer (agent nil)
	  searcher (agent nil)]
      (send-off indexer start-indexing maildir indexfile)
      (send-off searcher handle-searches indexfile (Integer. port))
      (await indexer searcher))))
