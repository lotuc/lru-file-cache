(ns lotuc.lru-file-cache.file-lock
  (:require
   [clojure.core.async :as async]
   [clojure.java.io :as io]
   [lotuc.lru-file-cache.utils :as u])
  (:import
   [java.io RandomAccessFile]
   [java.nio.channels FileChannel FileLock]
   [java.nio.file StandardOpenOption]))

;;; There might be multiple processes using the same cache directory. We want only
;;; one of them actually watching the directory.

;;; File-based locking mechanism for coordinating access to shared resources
;;; across multiple processes.
;;;
;;; This implementation uses a combination of file locks and heartbeat mechanisms
;;; to ensure that only one process has ownership of a lock at any given time.
;;;
;;; The lock owner periodically updates the file's modification time as a heartbeat.
;;; If the heartbeat stops (file not updated for contend-ms milliseconds), other
;;; processes can attempt to acquire the lock.

(defn with-timeout
  "Execute function f with a timeout. Returns the result of f if it completes within
   timeout-ms milliseconds, or returns default-value if it times out or throws an exception."
  [timeout-ms default-value f]
  (let [result (promise)
        task (future (deliver result (f)))
        timeout-result (deref result timeout-ms default-value)]
    (when (not (realized? result))
      (future-cancel task))
    timeout-result))

(defn -locked-by-me?
  "Check if the lock file exists and contains our ID.
   Returns true if we own the lock, false otherwise."
  [lock-file me-id]
  (with-timeout 5000 false
    (fn []
      (let [file (io/file lock-file)]
        (and (.exists file)
             (.isFile file)
             (= (slurp file) me-id))))))

(defn -try-lock-with-file-lock
  "Returns true if lock was acquired, false otherwise."
  [lock-file me-id]
  (let [lock-file-obj (io/file lock-file)]
    (u/ensure-parents lock-file-obj)
    (with-timeout 10000 false
      (fn []
        (with-open [raf (RandomAccessFile. lock-file-obj "rw")
                    channel (.getChannel raf)]
          (if-let [^FileLock lock (.tryLock channel)]
            (try
              (.setLength raf 0)
              (.writeBytes raf me-id)
              (.force channel true)
              true
              (finally
                (when (and lock (.isValid lock))
                  (.release lock))))
            false))))))

(defn -try-lock-with-atomic-move
  [lock-file me-id]
  (with-timeout 10000 false
    (fn []
      (u/ensure-parents lock-file)
      (spit lock-file me-id)
      (u/ensure-parents lock-file)
      (and (u/atomic-move-file lock-file lock-file)
           (-locked-by-me? lock-file me-id)))))

(defn -try-lock
  "Attempts to acquire a lock file with the given ID.

   Uses a multi-layered approach:
   1. First checks if we already own the lock
   2. Tries to acquire using FileLock (most reliable)
   3. Falls back to atomic file move as a last resort

   Returns true if lock was acquired, false otherwise."
  [lock-file me-id]
  (let [f (java.io.File/createTempFile "lock-" "-lock")]
    (try
      (or (-locked-by-me? lock-file me-id)
          (-try-lock-with-file-lock lock-file me-id)
          (-try-lock-with-atomic-move lock-file me-id))
      (catch Exception e false)
      (finally (u/delete-file-if-exists f)))))

(defn -build-id
  "Create a unique process identifier that includes both a UUID and the
  JVM process ID"
  []
  (str (random-uuid) "-" (.. java.lang.management.ManagementFactory getRuntimeMXBean getName)))

(comment
  (-try-lock "/tmp/a.lock" "42")
  (-locked-by-me? "/tmp/a.lock" "42")
  (-locked-by-me? "/tmp/a.lock" "41")
  #_())

(defn watch-lock-file
  "Creates a lock file watcher that manages lock ownership with heartbeats.

  This function implements a cooperative locking mechanism where multiple
  processes can contend for a lock, with the current owner maintaining
  ownership through periodic heartbeats (file mtime updates).

  When lock is held by me:
  - Poll lock file every `poll-ms-locked-by-me` milliseconds
  - Update the file's `mtime` as a heartbeat to signal continued ownership

  When lock is not held by me:
  - Poll lock file every `poll-ms-locked-by-others` milliseconds
  - If the file's `mtime` hasn't changed for more than `contend-ms` milliseconds,
    attempt to acquire the lock (the previous owner is considered inactive)

  Parameters:
  - lock-file: Path to the lock file
  - options map:
    - me-id: Unique identifier for this process (defaults to UUID + JVM process ID)
    - poll-ms-locked-by-me: Polling interval when we own the lock (default: 60000ms)
    - poll-ms-locked-by-others: Polling interval when we don't own the lock (default: 10000ms)
    - contend-ms: Time after which an unchanged lock file is considered abandoned (default: 60000ms)

  Returns a map with two channels:
  - :data - Receives lock status updates as {:hold? boolean}
  - :ctrl - Send any value to this channel to stop the watcher"
  [lock-file
   {:keys [me-id
           poll-ms-locked-by-me
           poll-ms-locked-by-others
           contend-ms]
    :or {poll-ms-locked-by-me 60000
         poll-ms-locked-by-others 10000
         contend-ms (* 10000 6)}}]
  (let [me-id (or me-id (str (random-uuid) "-" (.. java.lang.management.ManagementFactory getRuntimeMXBean getName)))
        lock-file (io/file lock-file)
        ;; Use sliding buffer to ensure we only keep the most recent status
        data (async/chan (async/sliding-buffer 1))
        ctrl (async/chan)
        timeout-ms
        (fn [is-me] (if is-me poll-ms-locked-by-me poll-ms-locked-by-others))
        try-lock
        #(-try-lock lock-file me-id)
        locked-by-me?
        #(-locked-by-me? lock-file me-id)
        hb-lost?
        #(with-timeout 5000 true
           (fn []
             (if-some [mtime (u/get-file-mtime lock-file)]
               (let [current-time (System/currentTimeMillis)
                     time-diff (- current-time mtime)]
                 (> time-diff contend-ms))
               true)))

        cont?
        (fn [is-me]
          (async/go
            (async/alt!
              (async/timeout (timeout-ms is-me)) ([_] true)
              ctrl ([v] (some? v)))))

        ;; Initial lock acquisition attempt
        is-me
        (if (and (.exists lock-file) (not (hb-lost?)))
          (locked-by-me?)
          (try-lock))]
    ;; Start the main watcher process
    (async/go
      (when is-me
        (async/<! (async/thread (u/update-file-mtime lock-file))))
      (async/>! data {:hold? (boolean is-me)})
      (loop [is-me is-me]
        (if (async/<! (cont? is-me))
          (let [is-me'
                (or (async/<! (async/thread (locked-by-me?)))
                    (if (async/<! (async/thread (hb-lost?)))
                      (async/<! (async/thread (try-lock)))
                      false))]
            ;; Update heartbeat if we hold the lock
            (when is-me'
              (async/<! (async/thread (u/update-file-mtime lock-file))))
            ;; Notify of lock status changes
            (when (not= is-me is-me')
              (async/>! data {:hold? (boolean is-me')}))
            (recur is-me'))
          (do
            ;; Clean up resources when exiting
            ;; Clean up by removing our lock file if we still own it
            (try
              (when (async/<! (async/thread (locked-by-me?)))
                (async/<! (async/thread (u/delete-file-if-exists lock-file))))
              (finally
                ;; Always close channels to prevent resource leaks
                (async/close! data)
                (async/close! ctrl)))))))
    {:data data :ctrl ctrl}))
