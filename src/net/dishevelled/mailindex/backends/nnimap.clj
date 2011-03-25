(ns net.dishevelled.mailindex.backends.nnimap
  "IMAP mailindex backend"
  (:import (javax.mail Folder UIDFolder Part Message Session Store)
           (javax.mail.internet MimeMultipart)
           (java.io ByteArrayOutputStream)
           (com.sun.mail.imap IMAPMessage IMAPFolder))
  (:use [clojure.java.io :only [reader file]]
        [clojure.contrib.seq :only [find-first]]))

;;;; Example config.clj

;; {:indexfile "/home/aosborne/var/cache/mailindex"
;;  :port 4321
;;  :reindex-frequency 60000
;;  :max-results 1000
;;  :optimize-hour 4                       ; optimise at 4am
;;  :backends [{:name "gnus-mail"
;;              :backend net.dishevelled.mailindex.backends.nnimap
;;              :host "localhost"
;;              :username "aosborne"
;;              :password "secret"
;;              ;;:authfile "/home/aosborne/.authinfo"
;;              :statefile "/home/aosborne/var/cache/mailindex/state"}]}

;;;; Connecting


(defn parse-authinfo-line
  "Parses an authinfo line, returns [machine login password]."
  [line]
  (if-let [m (re-matches #"machine (\S+) login (\S+) password (\S+)" line)]
    (rest m)))


(defn read-authinfo
  "Read a password from an authinfo file."
  [f host username]
  (->> (map parse-authinfo-line (line-seq (reader f)))
       (find-first (fn [[h u p]] (and (= h host) (= u username))))
       (last)))


(defn #^Session new-mail-session []
  (Session/getDefaultInstance (java.util.Properties.)))


(defn imap-connect [config]
  (let [{:keys [host username password authfile]} config
        password (or password (read-authinfo authfile host username))
        session (new-mail-session)
        store (.getStore session "imap")]
    (.connect store host username password)
    store))


(defn check-connection!
  "Checks whether our imap connection is open and if not opens it."
  [conn]
  (let [#^Store store (:store @conn)]
    (if (and store (.isConnected store))
      conn
      (swap! conn assoc :store (imap-connect (:config @conn))))))


;;;; Folder walking

(defn list-folder
  "A seq of the subfolders of a folder."
  [#^Folder folder]
  (seq (.list folder)))

(defn folder-tree
  "A seq of the entire folder tree in a mail store."
  [#^Store store]
  (tree-seq list-folder list-folder (.getDefaultFolder store)))

(defn holds-messages?
  "Returns true when a folder can hold messages."
  [#^Folder folder]
  (pos? (bit-and (.getType folder) Folder/HOLDS_MESSAGES)))

(defn message-folders
  "A seq of all the message folders in a store's entire folder tree."
  [store]
  (filter holds-messages? (folder-tree store)))


;;;; Message parsing

(defn message-id [#^IMAPMessage message]
  (let [#^IMAPFolder folder (.getFolder message)]
    {:group (.getFullName folder)
     :num (str (.getUID folder message))}))


(defn parse-message [#^IMAPMessage message]
  (let [message-bytes (ByteArrayOutputStream.)]
    (.writeTo message message-bytes)
    {:id (message-id message)
     :content (.toByteArray message-bytes)}))


(defmacro when-consumed
  "Modifies a lazy seq so that when it has been fully consumed, body is executed
  (for side-effects)."
  [s & body]
  `(concat
    ~s
    (lazy-seq ~@body nil)))


(defn read-state [conn]
  (try
   (with-open [r (reader (-> @conn :config :statefile))]
     (read (java.io.PushbackReader. r)))
   (catch java.io.FileNotFoundException e
     {})))


(defn update-folder
  "Returns a lazy-seq of messages in this folder."
  [#^Folder folder conn]
  (let [name (.getFullName folder)
        start-uid (get (:last-uids @conn) name 1)
        end-uid (.getUIDNext folder)]
    (.open folder Folder/READ_ONLY)
    (let [messages (map parse-message (.getMessagesByUID
                                       folder start-uid end-uid))]
      (when-consumed messages
                     (when (.isOpen folder)
                       (.close folder false))
                     (swap! conn assoc-in [:last-uids name] end-uid)
                     (spit (-> @conn :config :statefile)
                           (prn-str (:last-uids @conn)))))))


(defn updated-messages [conn & [index-mtime]]
  (check-connection! conn)
  (swap! conn assoc :last-uids (read-state conn))
  (mapcat #(update-folder % conn) (message-folders (:store @conn))))


(defn get-deletions [conn ids]
  (check-connection! conn)
  (let [#^Store store (:store @conn)]
    (for [id ids
          :let [#^IMAPFolder folder (.getFolder store (:group id))]
          :when (.getMessageByUID folder (Long/parseLong #^String (:num id)))]
      id)))


(defn get-connection [config]
  (atom {:new-messages-fn updated-messages
         :deleted-messages-fn get-deletions}))



