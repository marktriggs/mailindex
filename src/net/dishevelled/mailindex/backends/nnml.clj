(ns net.dishevelled.mailindex.backends.nnml
  (:import (java.util Date))
  (:use clojure.contrib.duck-streams))



(def *max-lines* 2000)

(defn- mtime-to-days [mtime]
  (let [now (.getTime (Date.))]
    (int (/ (- now mtime) 1000 60 60 24))))


(defn message-seq
  "A full lazy file reader that won't return more than `*max-lines*'
Fully lazy--doesn't even open the file until the first line is requred."
  [msg]
  (lazy-seq
    (let [#^java.io.BufferedReader rdr (reader msg)
	  step (fn step [#^java.io.BufferedReader rdr cnt]
		 (let [line (.readLine rdr)
		       cnt (int cnt)]
		   (if (and (< cnt (int *max-lines*))
			    line)
		     (cons line (lazy-seq (step rdr (inc cnt))))
		     (do (.close rdr)
			 nil))))]
      (step rdr 0))))


(defn filename-to-id
  "Parse a filesystem path into a group id (group + number)"
  [base filename]
  (let [[whole group num]
        (first
         (re-seq (re-pattern (str base "/*" "(.*?)" "/" "([0-9]+)$"))
                 filename))]
    {:group group :num num}))


(defn get-deletions
  "Return the subset of `ids' that represent deleted messages."
  [connection ids]
  (let [base (-> @connection :config :base)]
    (filter #(not (.exists (java.io.File.
                            (str base "/" (:group %) "/" (:num %)))))
            ids)))


(defn updated-messages
  "Find any messages that have been updated"
  [connection & [index-mtime]]
  (let [base (-> @connection :config :base)
        cmd ["find" base "-type" "f"]
        cmd (if index-mtime
              (concat cmd ["-mtime" (str "-" (inc (mtime-to-days index-mtime)))])
              cmd)
        messages (filter #(re-find #"/[0-9]+$" %)
                         (line-seq (reader (.. Runtime
                                               getRuntime
                                               (exec (into-array cmd))
                                               getInputStream))))
        new-messages (clojure.set/difference (set messages)
                                             (:seen-messages @connection))]
    (swap! connection update-in
           [:seen-messages]
           clojure.set/union new-messages)
    (map (fn [msg]
           {:id (filename-to-id base msg)
            :lines (message-seq msg)})
         new-messages)))



(defn get-connection [config]
  (atom {:new-messages-fn updated-messages
         :deleted-messages-fn get-deletions}))
