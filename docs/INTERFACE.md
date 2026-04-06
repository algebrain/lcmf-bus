# Интерфейс `lcmf-bus`

Этот документ описывает публичный интерфейс библиотеки
[`lcmf-bus`](https://github.com/algebrain/lcmf-bus).

Библиотека реализует app-level шину сообщений для
[`LCMF`](https://github.com/algebrain/lcmf-docs).

## Назначение

`lcmf-bus` дает:

- in-memory шину сообщений;
- дисциплину конвертов;
- обязательный `:module` при публикации;
- продолжение причинно-следственной цепочки через `:parent-envelope`;
- изоляцию ошибок слушателей.

Библиотека не включает:

- persistent delivery;
- buffering и backpressure;
- транзакционную обработку;
- worker pool;
- transport-адаптеры.

## Конверт сообщения

`publish!` возвращает конверт сообщения.

Практически полезная форма:

```clojure
{:event-type :booking/created
 :module :booking
 :payload {:id "b-1" :user-id "u-alice" :slot-id "slot-09-00"}
 :message-id "..."
 :correlation-id "..."
 :causation-path []
 :created-at 1710000000}
```

Если сообщение публикуется с `:parent-envelope`, новый конверт:

- наследует `:correlation-id`;
- расширяет `:causation-path`.

## `make-bus`

Создает экземпляр шины.

Общая форма:

```clojure
(make-bus)
(make-bus :logger logger-fn)
(make-bus :logger logger-fn :max-depth 8)
```

Опции первой версии:

- `:logger` — optional `(fn [level data] ...)`
- `:max-depth` — optional ограничение длины `causation-path`

Пример:

```clojure
(ns my.app
  (:require [lcmf.bus :as bus]))

(def app-bus
  (bus/make-bus))
```

## `subscribe!`

Подписывает обработчик на событие.

Общая форма:

```clojure
(subscribe! bus event-type handler)
(subscribe! bus event-type handler opts)
```

Где `handler` обычно имеет вид:

```clojure
(fn [bus envelope] ...)
```

`opts` первой версии:

- `:meta` — optional map для последующего `unsubscribe!`

Возвращаемое значение:

- `subscription-id`

Пример:

```clojure
(def booking-subscription
  (bus/subscribe! app-bus
                  :booking/created
                  (fn [_ envelope]
                    (println "booking created" (:payload envelope)))))
```

## `publish!`

Публикует сообщение в шину.

Общая форма:

```clojure
(publish! bus event-type payload opts)
```

В `opts` первой версии важны:

- `:module` — обязателен
- `:parent-envelope` — optional, если сообщение производно от другого
- `:correlation-id` — optional для root message

Пример root message:

```clojure
(bus/publish! app-bus
              :booking/created
              {:id "b-1" :user-id "u-alice" :slot-id "slot-09-00"}
              {:module :booking})
```

Пример derived message:

```clojure
(defn on-booking-created [bus envelope]
  (bus/publish! bus
                :notify/booking-created
                {:booking-id (get-in envelope [:payload :id])}
                {:module :notify
                 :parent-envelope envelope}))
```

Важно:

- `publish!` в текущей версии работает синхронно;
- слушатели вызываются в рамках одного вызова `publish!`;
- ошибка одного слушателя не останавливает остальных.

## `unsubscribe!`

Снимает подписку.

Общая форма:

```clojure
(unsubscribe! bus event-type matcher)
```

`matcher` первой версии может быть:

- `subscription-id`
- handler function
- `:meta` matcher map

Примеры:

```clojure
(bus/unsubscribe! app-bus :booking/created booking-subscription)
```

```clojure
(bus/unsubscribe! app-bus :booking/created {:slot :secondary})
```

## `listener-count`

Отладочный helper для подсчета слушателей.

Общая форма:

```clojure
(listener-count bus)
(listener-count bus event-type)
```

Пример:

```clojure
(bus/listener-count app-bus)
(bus/listener-count app-bus :booking/created)
```

## Минимальный walkthrough

```clojure
(ns my.app
  (:require [lcmf.bus :as bus]))

(def app-bus (bus/make-bus))

(def subscription-id
  (bus/subscribe! app-bus
                  :booking/created
                  (fn [bus envelope]
                    (bus/publish! bus
                                  :notify/booking-created
                                  {:booking-id (get-in envelope [:payload :id])}
                                  {:module :notify
                                   :parent-envelope envelope}))))

(bus/publish! app-bus
              :booking/created
              {:id "b-1" :user-id "u-alice" :slot-id "slot-09-00"}
              {:module :booking})

(bus/unsubscribe! app-bus :booking/created subscription-id)
```
