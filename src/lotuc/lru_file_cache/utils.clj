(ns lotuc.lru-file-cache.utils
  (:require
   [clojure.java.io :as io])
  (:import
   [java.nio.file
    CopyOption
    FileAlreadyExistsException
    Files
    LinkOption
    Paths
    StandardCopyOption]
   [java.nio.file.attribute FileAttribute FileTime]
   [java.time Instant]))

(defmacro catch-all [& body]
  `(try ~@body (catch Throwable _# (binding [*out* *err*] (println _#)) nil)))

(defn md5-hash
  "Calculate MD5 hash for a file - lowercase."
  [file]
  (let [algorithm (java.security.MessageDigest/getInstance "MD5")
        buffer (byte-array 8192)]
    (with-open [input-stream (io/input-stream file)]
      (loop [bytes-read (.read input-stream buffer)]
        (when (pos? bytes-read)
          (.update algorithm buffer 0 bytes-read)
          (recur (.read input-stream buffer)))))
    (let [hash-bytes (.digest algorithm)]
      (apply str (map #(format "%02x" (bit-and % 0xff)) hash-bytes)))))

(defn md5-string-like? [s]
  (= 32 (count s)))

(defn date-now []
  (java.util.Date.))

(defn date-now+millis [millis]
  (java.util.Date. (+ (.getTime (date-now)) millis)))

(defn -path [file-path]
  (let [p (if (string? file-path) file-path (str file-path))]
    (Paths/get p (into-array String []))))

(defn update-file-mtime [file-path]
  (try (let [t (FileTime/from (Instant/now))]
         (Files/setLastModifiedTime (-path file-path) t)
         (.toMillis t))
       (catch Exception _ nil)))

(defn get-file-mtime [file-path]
  (try (.toMillis (Files/getLastModifiedTime
                   (-path file-path)
                   (into-array LinkOption [])))
       (catch Throwable _ nil)))

(defn get-file-size [file-path]
  (try (Files/size (-path file-path))
       (catch Throwable _ nil)))

(defn copy-file-overwrite [src dst]
  (try (Files/copy
        (-path src)
        (-path dst)
        (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING]))
       true
       (catch Throwable _ false)))

(defn atomic-move-file [src dst]
  (try (Files/move
        (-path src)
        (-path dst)
        (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))
       true
       (catch FileAlreadyExistsException _ false)
       (catch Throwable _ false)))

(defn delete-file-if-exists
  "This can also be used to delete a empty directory."
  [file-path]
  (try (Files/deleteIfExists (-path file-path))
       (catch Throwable _ false)))

(defn ensure-parents [file-path]
  (try (Files/createDirectories (.getParent (-path file-path))
                                (into-array FileAttribute []))
       true
       (catch Throwable _ nil)))

(comment
  (md5-hash "/tmp/a.log")
  (md5-string-like? (md5-hash "/tmp/a.log"))
  (md5-hash "/tmp/a.log")
  (md5-string-like? (md5-hash "/tmp/a.log"))
  (copy-file-overwrite "/tmp/a.csv1" "/tmp/a.csv")
  (update-file-mtime "/tmp/a.log")
  (get-file-mtime "/tmp/a.log")
  (get-file-size "/tmp/a.log")
  (atomic-move-file "/tmp/a.log" "/tmp/a.log1")
  (delete-file-if-exists "/tmp/a.log1"))
