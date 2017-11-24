(ns net.dishevelled.mailindex.fieldpool
  (:import (org.apache.lucene.document Field TextField StringField SortedDocValuesField Field$Store)
           (org.apache.lucene.util BytesRef)))

(def field-pools (ref {}))

(defn- new-pool [^String name store tokenize]
  "Create a new pool of Field objects for a given field type."
  (let [construct-field (if tokenize
                          (fn [] (TextField. name "" (if store Field$Store/YES Field$Store/NO)))
                          (fn [] (StringField. name "" (if store Field$Store/YES Field$Store/NO))))]
    (ref {:next-available 0
          :fields (repeatedly construct-field)})))


(defn- next-field [pool]
  "Return a Field object from a pool, re-using an old one where possible."
  (dosync
   (let [i (:next-available @pool)]
     (alter pool update-in [:next-available] inc)
     (nth (:fields @pool) i))))


(defn- make-field [name store tokenize & [val]]
  "Return a new field object."
  (dosync
   (when-not (@field-pools name)
     (alter field-pools assoc name (new-pool name store tokenize))))

  (let [^Field field (next-field (@field-pools name))]
    (when val (.setStringValue field ^String val))
    field))


(defn reset []
  "Mark all Field objects as available for re-use."
  (doseq [pool (vals @field-pools)]
    (dosync
     (alter pool assoc :next-available 0))))


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

