(ns lotuc.lru-file-cache.lru
  (:require
   [clojure.core.async :as async]
   [clojure.data.priority-map :as pm]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [lotuc.lru-file-cache.cache-dir :as cache-dir]
   [lotuc.lru-file-cache.file-lock :as file-lock]
   [lotuc.lru-file-cache.utils :as u]))

(defn -record [{:keys [!lru]} md5 mtime size]
  (->> (fn [m]
         (let [tracked? (get-in m [:lru-map md5])]
           (cond-> (-> m
                       (assoc-in [:lru-map md5] mtime)
                       (assoc-in [:size-map md5] (or size 0)))
             (not tracked?)
             (update :size #(+ (or % 0) (or size 0))))))
       (swap! !lru)))

(defn -record-removal [{:keys [!lru]} md5]
  (->> (fn [m]
         (let [size (or (get-in m [:size-map md5]) 0)]
           (-> m
               (update :lru-map dissoc md5)
               (update :size-map dissoc md5)
               (update :size #(- (or % 0) size)))))
       (swap! !lru)))

(defn -schedule-run
  ([run-once! close-promise !state opts]
   (-schedule-run run-once! close-promise !state opts false))
  ([run-once! close-promise !state {:keys [interval-ms] :as opts} stop-run?]
   (letfn [(run1 []
             (swap! !state assoc :start-time (u/date-now))
             (run-once!)
             (swap! !state assoc :end-time (u/date-now))
             :done)
           (stop! []
             (swap! !state assoc :stop? true)
             (let [{:keys [current-run stop?]} @!state]
               (some-> current-run deref)))
           (start! []
             (locking !state
               (stop!)
               (swap! !state assoc :current-run (future (run1)) :stop? false)))
           (end-loop []
             (async/go
               (swap! !state assoc :cancel-time (u/date-now) :loop-run-stop? true)
               (async/<! (async/thread (stop!)))))
           (wait-for-next [ctrl]
             (async/go
               (when (pos? interval-ms)
                 (swap! !state assoc
                        :next-start-time (u/date-now+millis interval-ms))
                 (async/alt!
                   (async/timeout interval-ms) ([_] true)
                   ctrl ([_] (async/<! (end-loop)) false)))))
           (loop-run! [ctrl]
             (async/thread @close-promise (async/close! ctrl))
             (async/go-loop []
               (async/alt!
                 (async/thread (start!))
                 ([_] (when (async/<! (wait-for-next ctrl)) (recur)))
                 ctrl
                 ([_] (async/<! (end-loop))))))]
     (if stop-run?
       (when-some [{:keys [stop-loop-run!]} @!state]
         (when stop-loop-run! (stop-loop-run!)))
       (let [ch (async/chan)]
         (reset! !state {:stop-loop-run
                         (fn* ^:once stop-loop-run [] (async/close! ch))})
         (loop-run! ch))))))

(defn -scan-files-dir
  "Build lru base on the mtime."
  ([cache]
   (-scan-files-dir cache (or (:!scan-state cache) (atom {}))))
  ([cache !state]
   (loop [i 1
          [f & fs] (file-seq (cache-dir/-md5-cache-dir cache))]
     (if (or (:stop? @!state) (not f))
       (swap! !state dissoc :scanning)
       (do
         (when (and f (.isFile f))
           (swap! !state assoc :scanned-count i :scanning f)
           (let [md5
                 (let [n (.getName ^java.io.File f)]
                   (when (u/md5-string-like? n) n))

                 [mtime file-size]
                 (when md5
                   [(u/get-file-mtime f)
                    (u/get-file-size f)])]
             (-record cache md5 mtime file-size)))
         (recur (inc i) fs))))))

(defn -lru-scan
  ([cache]
   (-lru-scan cache false))
  ([{:keys [!scan-state scan-opts close] :as cache} stop-scan?]
   (let [scan-opts (update scan-opts :interval-ms #(or % 36000000))]
     (-schedule-run
      (fn* scan-files ^:once [] (-scan-files-dir cache))
      close !scan-state scan-opts stop-scan?))))

(declare lru-evict)

(defn -lru-cleanup
  "Evict files if necessary to stay under max-size."
  ([cache]
   (-lru-cleanup cache false))
  ([{:keys [!lru !cleanup-state cleanup-opts max-size close] :as cache} stop-run?]
   (let [cleanup-opts (update cleanup-opts :interval-ms #(or % 20000))]
     (letfn [(cleanup! []
               (loop [i 1 {:keys [size]} @!lru]
                 (when-let [md5 (and (not (:stop? @!cleanup-state))
                                     size max-size (pos? max-size) (> size max-size)
                                     (lru-evict cache))]
                   (->> (fn [s]
                          (-> s
                              (assoc :removed-count i)
                              (assoc :cleaning md5)
                              (update :total-removed-count (fnil inc 0))))
                        (swap! !cleanup-state))
                   (recur (inc i) @!lru))))]
       (-schedule-run cleanup! close !cleanup-state cleanup-opts stop-run?)))))

(defn -lru-scan-lock-watch!
  ([cache]
   (-lru-scan-lock-watch! cache nil))
  ([{:keys [!lock-state scan-lock close]
     :as cache}
    {:keys [on-hold? on-lost?]}]
   (let [on-hold? (or on-hold? identity)
         on-lost? (or on-lost? identity)
         {:keys [lock-file watch-opts]} scan-lock
         {:keys [ctrl data]} (file-lock/watch-lock-file lock-file watch-opts)
         checked? (promise)]
     (when-not (:watching? @!lock-state)
       (swap! !lock-state assoc :hold? nil :watching? true)
       (async/go-loop [i 0]
         (when (= i 1) (deliver checked? true))
         (if-some [{:keys [hold?]} (async/<! data)]
           (do
             (async/<! (async/thread (if hold? (on-hold?) (on-lost?))))
             (swap! !lock-state assoc
                    :hold? hold? :switch-time (u/date-now))
             (recur (inc i)))
           (do (async/<! (async/thread (on-lost?)))
               (swap! !lock-state assoc
                      :hold? false :watching? false :switch-time (u/date-now)))))
       (async/go (async/<! (async/thread @close (async/close! ctrl))))
       checked?))))

(defn lru-evict
  ([{:keys [!lru] :as cache}]
   (when-some [[md5 mtime] (first (:lru-map @!lru))]
     (if-some [f (cache-dir/-cache-file cache md5 true)]
       (let [mtime' (u/get-file-mtime f)
             size   (u/get-file-size f)]
         (if (or (= mtime mtime') (not size))
           ;; mtime not changed, but cannot get size for some reason, evict
           ;; first for later rescan
           (lru-evict cache md5)
           ;; mtime changed, re-record it & retry eviction
           (do (-record cache md5 mtime' size)
               (recur cache))))
       ;; file not found, cleanup it
       (lru-evict cache md5))))
  ([{:keys [!lru] :as cache} md5]
   (-record-removal cache md5)
   (cache-dir/remove-file-by-md5 cache md5)
   md5))

(defn -lru-put [{:keys [!lru] :as cache} put-fn]
  (when-some [{:keys [md5 new? mtime size]} (put-fn cache)]
    (-record cache md5 mtime size)
    md5))

(defn lru-put [{:keys [!lru] :as cache} file]
  (-lru-put cache #(cache-dir/cache-file-by-md5 % file)))

(defn lru-put-content [{:keys [!lru] :as cache} content]
  (-lru-put cache #(cache-dir/cache-content-by-md5 % content)))

(defn lru-get [{:keys [!lru] :as cache} md5]
  (let [md5 (string/lower-case md5)
        {:keys [mtime f]} (cache-dir/get-file-by-md5 cache md5)]
    (when mtime
      (swap! !lru assoc-in [:lru-map md5] mtime))
    f))

(defn create-lru-cache
  [{:keys [cache-dir scan-lock] :as cache}]
  {:pre [(some? cache-dir)]}
  (let [scan-lock
        (update scan-lock :lock-file #(or % (io/file cache-dir "lock")))

        cache
        (assoc cache
               :!lru (atom {:lru-map (pm/priority-map)})
               :!scan-state (atom nil)
               :!lock-state (atom nil)
               :!cleanup-state (atom nil)
               :scan-lock scan-lock
               :close (promise))]
    (doto cache
      (-lru-scan-lock-watch!
       {:on-hold? #(do (-lru-scan cache)
                       (-lru-cleanup cache))
        :on-lost? #(do (-lru-scan cache true)
                       (-lru-cleanup cache true))}))))

(defn close-lru-cache!
  [{:keys [close]}]
  (some-> close (deliver true)))
