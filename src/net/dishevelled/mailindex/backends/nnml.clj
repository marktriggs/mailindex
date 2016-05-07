(ns net.dishevelled.mailindex.backends.nnml
  (:require [clojure.java.io :refer [file reader]]
            [net.dishevelled.mailindex :refer [debug]]
            [clojure.set])
  (:import (java.io InputStream FileInputStream ByteArrayOutputStream IOException)
           (java.util Date)
           (java.util.zip GZIPInputStream)))

(defn- mtime-to-days [mtime]
  (let [now (.getTime (Date.))]
    (int (/ (- now mtime) 1000 60 60 24))))


(defn input-stream-for-file [file]
  (let [fis (FileInputStream. file)]
    (if (.endsWith (.getName file) ".gz")
      (GZIPInputStream. fis)
      fis)))

(defn- message-bytes
  "Return the bytes of a message."
  [^String msg]
  (try
    (let [f (file msg)
          buf (byte-array 4096)
          out (ByteArrayOutputStream. (.length f))]
      ;; Our initial length is really just a guess here.  We'll be right for
      ;; uncompressed files and wrong for everything else :)
      (with-open [^InputStream is (input-stream-for-file f)]
        (loop [len (.read is buf)]
          (when (>= len 0)
            (.write out buf 0 len)
            (recur (.read is buf)))))
      (.toByteArray out))
    (catch IOException _
      (byte-array 0))))


(defn- filename-to-id
  "Parse a filesystem path into a group id (group + number)"
  [base filename]
  (let [[whole group num]
        (first
         (re-seq (re-pattern (str base "/*" "(.*?)" "/" "([0-9]+)(:?\\.gz)?$"))
                 filename))]
    {:group (.replace group "/" ".") :num num}))


(defn- id-to-filename
  "Turn a group id (group + number) back into a filesystem path"
  [base id]
  (str base "/" (.replace (:group id) "." "/") "/" (:num id)))



;;;
;;; Our public interface...
;;;

(defn get-deletions
  "Return the subset of `ids' that represent deleted messages."
  [connection ids]
  (debug "Checking deletions for connection: %s" @connection)
  (let [base (-> @connection :config :base)]
    (debug "Using base '%s' and working directory '%s'"
           base
           (System/getProperty "user.dir"))
    (filter #(let [path (id-to-filename base %)
                   path-exists? (or (.exists (java.io.File. path))
                                    (.exists (java.io.File. (str path ".gz"))))]
               (debug "Does '%s' exist?  %s" path path-exists?)
               (not path-exists?))
            ids)))


(defn updated-messages
  "Find any messages that have been updated"
  [connection & [index-mtime]]
  (let [base (-> @connection :config :base)
        cmd ["find" "-L" base "(" "-type" "f" "-o" "-type" "l" ")"]
        cmd (if index-mtime
              (concat cmd ["-mtime" (str "-" (inc
                                              (mtime-to-days index-mtime)))])
              cmd)
        messages (set (filter #(re-find #"/[0-9]+(\.gz)?$" %)
                              (line-seq (reader (.. Runtime
                                                    getRuntime
                                                    (exec (into-array cmd))
                                                    getInputStream)))))
        new-messages (clojure.set/difference messages
                                             (:seen-messages @connection))]

    ;; Add any newly-found messages to our list of seen messages.
    ;; Turf out any seen messages that we didn't re-find to avoid
    ;; unbounded growth.
    (swap! connection update-in
           [:seen-messages]
           (fn [seen-messages]
             (set (remove (fn [msg] (not (messages msg)))
                          (clojure.set/union seen-messages new-messages)))))

    (map (fn [msg]
           {:id (filename-to-id base msg)
            :content (message-bytes msg)})
         new-messages)))



(defn get-connection [config]
  (atom {:new-messages-fn updated-messages
         :deleted-messages-fn get-deletions}))
