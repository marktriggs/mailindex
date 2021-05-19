(ns net.dishevelled.mailindex.fieldpool
  (:import (org.apache.lucene.document Field TextField StringField SortedDocValuesField Field$Store)
           (org.apache.lucene.util BytesRef)))

(def ^ThreadLocal field-pools (ThreadLocal.))

(defn- new-pool [^String name store tokenize]
  "Create a new pool of Field objects for a given field type."
  (let [construct-field (if tokenize
                          (fn [] (TextField. name "" (if store Field$Store/YES Field$Store/NO)))
                          (fn [] (StringField. name "" (if store Field$Store/YES Field$Store/NO))))]
    {:pool (doto (java.util.HashMap.)
             (.put :next-available 0)
             (.put :fields (java.util.ArrayList.)))
     :construct-field construct-field}))


(defn- next-field [pool]
  "Return a Field object from a pool, re-using an old one where possible."
  (let [pool-map  ^java.util.HashMap (:pool pool)
        pool-fields ^java.util.ArrayList (:fields pool-map)
        idx (.get pool-map :next-available)]
    (.put pool-map :next-available (inc idx))
    (when (>= idx (count pool-fields))
      (.add pool-fields ((:construct-field pool))))
    (.get pool-fields idx)))


(defn- make-field [name store tokenize & [val]]
  "Return a new field object."
  (when-not (.get field-pools)
    (.set field-pools (java.util.HashMap.)))

  (let [^java.util.HashMap pools (.get field-pools)]
    (when-not (.containsKey pools name)
      (.put pools name (new-pool name store tokenize)))

    (let [^Field field (next-field (.get pools name))]
      (when val (.setStringValue field ^String val))
      field)))


(defn reset []
  "Mark all Field objects as available for re-use."
  (when-let [^java.util.HashMap pools (.get ^ThreadLocal field-pools)]
    (doseq [name (.keySet pools)]
      (.put ^java.util.HashMap (:pool (.get pools name)) :next-available 0))))


(defn sort-field [^String name ^String val]
  "Create a field we can sort on"
  (SortedDocValuesField. name (BytesRef. val)))


(defn tokenized-field [name val]
  "Create a tokenized, stored field."
  (make-field name true true val))

(defn untokenized-field [name val]
  "Create a tokenized, stored field."
  (make-field name false false val))


(defn tokenized-unstored-field [name val]
  "Create a tokenized, stored field."
  (make-field name false true val))


(defn stored-field [^String name ^String val]
  "Create a stored, untokenized field."
  (make-field name true false val))

