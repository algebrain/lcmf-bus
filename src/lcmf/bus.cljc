(ns lcmf.bus)

(defn- now-ms []
  #?(:clj  (System/currentTimeMillis)
     :cljs (.now js/Date)))

(defn- new-id []
  (str (random-uuid)))

(defn- log!
  [bus level data]
  (when-let [logger (:logger (:opts bus))]
    (try
      (logger level (merge {:component :lcmf.bus} data))
      (catch #?(:clj Throwable :cljs :default) _
        nil))))

(defn- ensure-module! [module]
  (when-not module
    (throw (ex-info "Missing required :module in publish options"
                    {:reason :missing-module}))))

(defn- listener-entries [bus event-type]
  (get @(:listeners bus) event-type []))

(defn- make-root-envelope
  [event-type payload {:keys [module correlation-id]}]
  {:event-type event-type
   :module module
   :payload payload
   :message-id (new-id)
   :correlation-id (or correlation-id (new-id))
   :causation-path []
   :created-at (now-ms)})

(defn- derive-envelope
  [event-type payload {:keys [module parent-envelope]} max-depth]
  (let [step [(:module parent-envelope) (:event-type parent-envelope)]
        causation-path (conj (vec (:causation-path parent-envelope)) step)]
    (when (some #(= % [module event-type]) causation-path)
      (throw (ex-info "Cycle detected in causation path"
                      {:reason :cycle-detected
                       :event-type event-type
                       :module module
                       :causation-path causation-path})))
    (when (and max-depth (> (count causation-path) max-depth))
      (throw (ex-info "Max causation depth exceeded"
                      {:reason :max-depth-exceeded
                       :event-type event-type
                       :module module
                       :max-depth max-depth
                       :causation-depth (count causation-path)})))
    {:event-type event-type
     :module module
     :payload payload
     :message-id (new-id)
     :correlation-id (:correlation-id parent-envelope)
     :causation-path causation-path
     :created-at (now-ms)}))

(defn- make-envelope
  [bus event-type payload {:keys [parent-envelope] :as opts}]
  (if parent-envelope
    (derive-envelope event-type payload opts (:max-depth (:opts bus)))
    (make-root-envelope event-type payload opts)))

(defn make-bus
  [& {:as opts}]
  {:opts opts
   :listeners (atom {})})

(defn subscribe!
  ([bus event-type handler]
   (subscribe! bus event-type handler nil))
  ([bus event-type handler opts]
   (let [subscription-id (new-id)
         entry {:id subscription-id
                :handler handler
                :meta (:meta opts)}]
     (swap! (:listeners bus) update event-type (fnil conj []) entry)
     subscription-id)))

(defn- meta-match?
  [expected actual]
  (every? (fn [[k v]] (= v (get actual k))) expected))

(defn unsubscribe!
  [bus event-type matcher]
  (let [removed? (atom false)]
    (swap! (:listeners bus)
           update
           event-type
           (fn [entries]
             (vec
              (remove
               (fn [{:keys [id handler meta]}]
                 (let [match? (cond
                                (string? matcher) (= matcher id)
                                #?(:clj (fn? matcher) :cljs (fn? matcher)) (= matcher handler)
                                (map? matcher) (meta-match? matcher meta)
                                :else false)]
                   (when match?
                     (reset! removed? true))
                   match?))
               entries))))
    @removed?))

(defn listener-count
  ([bus]
   (reduce + 0 (map count (vals @(:listeners bus)))))
  ([bus event-type]
   (count (listener-entries bus event-type))))

(defn publish!
  [bus event-type payload {:keys [module] :as opts}]
  (ensure-module! module)
  (let [envelope (make-envelope bus event-type payload opts)
        handlers (vec (listener-entries bus event-type))]
    (log! bus :info {:event :event-published
                     :event-type event-type
                     :module module
                     :correlation-id (:correlation-id envelope)
                     :listener-count (count handlers)})
    (doseq [{:keys [handler]} handlers]
      (try
        (handler bus envelope)
        (catch #?(:clj Throwable :cljs :default) ex
          (log! bus :error {:event :handler-failed
                            :event-type event-type
                            :module module
                            :correlation-id (:correlation-id envelope)
                            :exception ex}))))
    envelope))
