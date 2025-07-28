(ns lotuc.lru-file-cache.cache-dir
  (:require
   [clojure.java.io :as io]
   [lotuc.lru-file-cache.utils :as u]))

;;; Might be exception between the exist check & following operation. Here we
;;; handles some of such exception, try to get necessary data as whole.

(defn -cache-tmpfile [{:keys [cache-dir]}]
  {:pre [(or (string? cache-dir) (instance? java.io.File cache-dir))]}
  (->> (str "t" (+ 1000 (rand-int 9000))
            "-" (System/currentTimeMillis))
       (io/file cache-dir "tmp")))

(defn -md5-cache-dir [{:keys [cache-dir]}]
  {:pre [(or (string? cache-dir) (instance? java.io.File cache-dir))]}
  (io/file cache-dir "cache"))

(defn -cache-file
  ([cache md5]
   (-cache-file cache md5 false))
  ([cache md5 check-exist?]
   {:pre [(string? md5) (= 32 (count md5))]}
   (let [f (io/file (-md5-cache-dir cache) (subs md5 0 2) md5)]
     (when (or (not check-exist?) (.exists f)) f))))

(defn cache-by-md5
  "(write-to-file ^java.io.File file)"
  [{:keys [cache-dir] :as cache} write-to-file]
  {:pre [(or (string? cache-dir) (instance? java.io.File cache-dir))]}
  (u/catch-all
   (let [t (-cache-tmpfile cache)
         _ (u/ensure-parents t)
         _ (write-to-file t)
         md5 (u/md5-hash t)
         f (-cache-file cache md5)]
     (try (if (.exists f)
            {:mtime (u/update-file-mtime f)
             :new? false
             :md5 md5
             :size (u/get-file-size f)}
            (do (u/ensure-parents f)
                (let [moved-by-me (u/atomic-move-file t f)
                      mtime (u/get-file-mtime f)
                      size (u/get-file-size f)]
                  (when (.exists f)
                    {:mtime mtime
                     :new? moved-by-me
                     :md5 md5
                     :size size}))))
          (finally
            (u/delete-file-if-exists t))))))

(defn cache-content-by-md5
  [{:keys [cache-dir] :as cache} content]
  (cache-by-md5 cache #(spit % content)))

(defn cache-file-by-md5
  [{:keys [cache-dir] :as cache} file]
  {:pre [(or (string? cache-dir) (instance? java.io.File cache-dir))]}
  (cache-by-md5 cache #(u/copy-file-overwrite file %)))

(defn get-file-by-md5
  [{:keys [cache-dir] :as cache} md5]
  {:pre [(or (string? cache-dir) (instance? java.io.File cache-dir))]}
  (u/catch-all
   (let [f (-cache-file cache md5)]
     (when (.exists f)
       {:f f
        :mtime (u/update-file-mtime f)}))))

(defn remove-file-by-md5
  [{:keys [cache-dir] :as cache} md5]
  {:pre [(or (string? cache-dir) (instance? java.io.File cache-dir))]}
  (u/catch-all
   (let [f (-cache-file cache md5)
         b (u/delete-file-if-exists f)]
     (u/delete-file-if-exists (.getParentFile f))
     b)))
