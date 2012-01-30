(ns net.dishevelled.mailindex.backends.nnml
  (:import (java.util Date)
           (java.io FileInputStream IOException))
  (:require clojure.set)
  (:use clojure.java.io
        [net.dishevelled.mailindex :only [debug]]))


(defn- mtime-to-days [mtime]
  (let [now (.getTime (Date.))]
    (int (/ (- now mtime) 1000 60 60 24))))


(defn- message-bytes
  "Return the bytes of a message."
  [^String msg]
  (try
    (let [out (byte-array (.length (file msg)))]
      (with-open [fis (FileInputStream. msg)]
        (.read fis out))
      out)
    (catch IOException _
      (byte-array 0))))


(defn- filename-to-id
  "Parse a filesystem path into a group id (group + number)"
  [base filename]
  (let [[whole group num]
        (first
         (re-seq (re-pattern (str base "/*" "(.*?)" "/" "([0-9]+)$"))
                 filename))]
    {:group (.replace group "/" ".") :num num}))


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
    (filter #(let [path (str base "/" (:group %) "/" (:num %))
                   path-exists? (.exists (java.io.File. path))]
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
        messages (set (filter #(re-find #"/[0-9]+$" %)
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
