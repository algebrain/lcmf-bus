# Интерфейс `lcmf-bus`

Этот документ описывает публичный интерфейс библиотеки
[`lcmf-bus`](https://github.com/algebrain/lcmf-bus).

Библиотека реализует общую шину сообщений уровня приложения для
[`LCMF`](https://github.com/algebrain/lcmf-docs).

## Назначение

`lcmf-bus` дает:

- шину сообщений в памяти;
- дисциплину конвертов;
- обязательный `:module` при публикации;
- продолжение причинно-следственной цепочки через `:parent-envelope`;
- раннее отклонение ошибок на границе публичных функций;
- изоляцию ошибок слушателей.

Библиотека не включает:

- устойчивую доставку после перезапуска;
- буферизацию и управление обратным давлением;
- транзакционную обработку;
- пул рабочих потоков;
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

- `:logger` — необязательная журналирующая функция `(fn [level data] ...)`
- `:max-depth` — необязательное ограничение длины `causation-path`

Если `:logger` передан, он должен быть функцией.
Если `:max-depth` передан, он должен быть положительным целым числом.

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

- `:meta` — необязательная карта для последующего `unsubscribe!`

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
- `:parent-envelope` — необязателен, если сообщение производно от другого
- `:correlation-id` — необязателен для корневого сообщения

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

## Проверка границы API

Шина отклоняет некорректные аргументы сразу, до изменения состояния подписок
или сборки нового конверта.

Единый вид ошибки аргумента:

```clojure
(throw (ex-info "Invalid argument"
                {:reason :invalid-argument
                 :field :event-type
                 :value "\"booking/created\""}))
```

Единый вид ошибки производного конверта:

```clojure
(throw (ex-info "Invalid parent envelope"
                {:reason :invalid-parent-envelope
                 :parent-envelope {:event-type :booking/created}}))
```

Проверки публичных функций:

| Функция | Проверка |
|---------|----------|
| `make-bus` | `:max-depth`, если передан, должен быть положительным целым числом; `:logger`, если передан, должен быть функцией |
| `publish!` | `event-type` должен быть keyword; `opts` должен быть картой; `:module` обязателен; `:parent-envelope`, если передан, должен иметь корректную форму |
| `subscribe!` | `event-type` должен быть keyword; `handler` должен быть функцией; `opts`, если передан, должен быть картой |
| `unsubscribe!` | `event-type` должен быть keyword; `matcher` должен быть строкой `subscription-id`, функцией обработчика или непустой картой для сверки `:meta` |
| `listener-count` | `event-type`, если передан, должен быть keyword |

Минимальная корректная форма `:parent-envelope`:

```clojure
{:module :booking
 :event-type :booking/created
 :correlation-id "c-1"
 :causation-path []}
```

`:causation-path` должен быть вектором. Каждый элемент пути должен быть
векторной парой:

```clojure
[:booking :booking/created]
```

Первый элемент пары — идентификатор модуля, второй элемент — тип события.

## `unsubscribe!`

Снимает подписку.

Общая форма:

```clojure
(unsubscribe! bus event-type matcher)
```

`matcher` первой версии может быть:

- `subscription-id`;
- функция обработчика;
- непустая карта для сверки `:meta`.

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
