# Практична робота 4 — RabbitMQ (варіант 5)

Брокер повідомлень RabbitMQ: Default, Direct і Topic обмінники. C#, .NET 8, RabbitMQ.Client 6.8.1.

## Запуск

Потрібні .NET SDK 8 і Docker.

```
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Вебінтерфейс: http://localhost:15672 (guest / guest).

Кожну програму запускати в окремому терміналі: спочатку Consumer'и, потім Producer.
Для завдань 2 і 3 спершу виконати `dotnet build` у папці завдання, бо кілька Consumer'ів запускаються з одного проєкту.

## Структура

```
Task1.DefaultExchange/
  Publisher/Program.cs   — надсилає замовлення в чергу orders-queue кожні 1,5 с
  Consumer/Program.cs    — читає orders-queue і рахує отримані повідомлення
Task2.DirectExchange/
  Producer/Program.cs    — створює обмінник orders_direct, 3 черги, прив'язки і надсилає 20 повідомлень
  Consumer/Program.cs    — читає чергу, назва якої передається аргументом
Task3.TopicExchange/
  Producer/Program.cs    — створює обмінник products_topic, черги категорій і надсилає 20 товарів
  Consumer/Program.cs    — читає чергу категорії, назва категорії передається аргументом
```

Черги оголошені з `durable: true`, бо RabbitMQ 4 не дозволяє непостійні неексклюзивні черги.

## Завдання 1 — Default Exchange

Змінено текст повідомлень, назву черги (`orders-queue`) та інтервал (1500 мс); Consumer виводить кількість отриманих повідомлень.

```
cd Task1.DefaultExchange/Consumer && dotnet run
cd Task1.DefaultExchange/Publisher && dotnet run
```

![Publisher](<Task1.DefaultExchange/Знімок екрана 2026-11-10 214956.png>)
![Consumer](<Task1.DefaultExchange/Знімок екрана 2026-11-10 215035.png>)

## Завдання 2 — Direct Exchange

Обмінник `orders_direct`, черги `new-order`, `order-update`, `order-cancelled`.
Додаткова прив'язка: ключ `order` теж веде в чергу `new-order`.

```
cd Task2.DirectExchange/Consumer && dotnet run --no-build -- new-order
cd Task2.DirectExchange/Consumer && dotnet run --no-build -- order-update
cd Task2.DirectExchange/Consumer && dotnet run --no-build -- order-cancelled
cd Task2.DirectExchange/Producer && dotnet run --no-build
```

Результат: `new-order` — 10 повідомлень (5 `new-order` + 5 `order`), `order-update` — 5, `order-cancelled` — 5.

![Producer](<Task2.DirectExchange/Знімок екрана 2026-11-10 220219.png>)
![new-order](<Task2.DirectExchange/Знімок екрана 2026-11-10 220241.png>)
![order-update](<Task2.DirectExchange/Знімок екрана 2026-11-10 220257.png>)
![order-cancelled](<Task2.DirectExchange/Знімок екрана 2026-11-10 220314.png>)
![Bindings](<Task2.DirectExchange/Знімок екрана 2026-11-10 221415.png>)

## Завдання 3 — Topic Exchange

Обмінник `products_topic`, черга на кожну категорію: electronics, clothing, beauty, books.
Прив'язка за ключем `product.{category}`.

```
cd Task3.TopicExchange/Consumer && dotnet run --no-build -- electronics
cd Task3.TopicExchange/Consumer && dotnet run --no-build -- clothing
cd Task3.TopicExchange/Consumer && dotnet run --no-build -- beauty
cd Task3.TopicExchange/Consumer && dotnet run --no-build -- books
cd Task3.TopicExchange/Producer && dotnet run --no-build
```

Результат: з 20 повідомлень 18 потрапили у свої черги (5 / 5 / 4 / 4).
Ключі `product.toys` (немає такої черги) і `product.electronics.sale` (три слова замість двох) нікуди не потрапили — Producer виводить для них `NOT ROUTED`.

![Producer](<Task3.TopicExchange/Знімок екрана 2026-11-10 221658.png>)
![electronics](<Task3.TopicExchange/Знімок екрана 2026-11-10 221716.png>)
![clothing](<Task3.TopicExchange/Знімок екрана 2026-11-10 221731.png>)
![beauty](<Task3.TopicExchange/Знімок екрана 2026-11-10 221748.png>)
![books](<Task3.TopicExchange/Знімок екрана 2026-11-10 221801.png>)

## Висновки

У ході роботи я ознайомилася з брокером повідомлень RabbitMQ та організацією асинхронного обміну повідомленнями між Producer і Consumer.

- **Default Exchange** надсилає повідомлення напряму в чергу, назва якої збігається з ключем маршрутизації. Producer і Consumer працюють незалежно: повідомлення зберігаються в черзі, доки Consumer їх не прочитає.
- **Direct Exchange** спрямовує повідомлення в черги, прив'язані з точно таким самим ключем. Одна черга може мати кілька прив'язок: черга `new-order` отримувала повідомлення і з ключем `new-order`, і з ключем `order` (10 повідомлень), а `order-update` і `order-cancelled` — тільки свої (по 5).
- **Topic Exchange** порівнює ключ із шаблоном прив'язки по словах, розділених крапкою. З 20 повідомлень 18 потрапили у черги своїх категорій, а `product.toys` (немає черги) і `product.electronics.sale` (три слова замість двох) не підійшли під жоден шаблон `product.{category}` і не були доставлені.

Отже, тип обмінника визначає, як саме повідомлення розподіляються між чергами: Default — за назвою черги, Direct — за точним збігом ключа, Topic — за шаблоном.
