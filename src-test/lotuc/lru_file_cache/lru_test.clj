(ns lotuc.lru-file-cache.lru-test
  (:require
   [clojure.test :refer :all]
   [lotuc.lru-file-cache.eviction :as eviction]))

(deftest record-test
  (testing "record & record removal"
    (let [!lru (atom {})
          cache {:!lru !lru}]
      (eviction/-record cache "md5-0" 0 42)
      (is (= {:lru-map {"md5-0" 0} :size 42} @!lru))

      (eviction/-record cache "md5-0" 1 42)
      (is (= {:lru-map {"md5-0" 1} :size 42} @!lru))

      (eviction/-record cache "md5-1" 2 20)
      (is (= {:lru-map {"md5-0" 1 "md5-1" 2} :size 62} @!lru))

      (eviction/-record-removal cache "md5-0" 42)
      (is (= {:lru-map {"md5-1" 2} :size 20} @!lru)))))
