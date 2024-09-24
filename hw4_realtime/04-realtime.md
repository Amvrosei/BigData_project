# Домашнее задание по Spark Advanced и Realtime

* Deadline: 06.10.2024, 23:59

#### Комментарии 

* Задачу 1 можно выполнять как на RDD, так и на DF API. Но лучше на DF как на более высокоуровненом и современном. 
* Задачу 2 - только на Dstream API (аналог RDD для Spark Streaming).

## Задача 1. Spark advanced
#### Исходные данные

* `/data/wiki/en_articles_part` - статьи Википедии. Засылать на тестирование нужно на частичном датасете (чтоб не перегружать кластер).

Формат данных:
```
article ID <tab> article text
```
* `/data/wiki/stop_words_en-xpo6.txt` - список стоп-слов для 2-й задачи.

Формат данных: одно стоп-слово на строчку
```
...
wherein
whereupon
wherever
...
```
#### Условие 

Задача состоит в извлечении коллокаций. Это комбинации слов, которые часто встречаются вместе. Например, «High school» или «Roman Empire». Чтобы найти совпадения, нужно использовать метрику NPMI (нормализованная точечная взаимная информация).

PMI двух слов a и b определяется как:
```math
\textcolor{blue}{PMI \lparen a,b \rparen = ln \bigg( {\frac {P(a,b)}{P(a)*P(b)}} \bigg)}
```
, где `P(ab)` - вероятность двух слов, идущих подряд, а `P(a)` и `P (b)` - вероятности слов a и b соответственно.

Вам нужно будет оценить вероятности встречаемости слов, то есть:
```math
P(a)=\frac{num\_of\_occurrences\_of\_word\_"a"}{num\_of\_occurrences\_of\_all\_words}
```
```math
P(ab)=\frac{num\_of\_occurrences\_of\_pair\_"ab"}{num\_of\_occurrences\_of\_all\_pairs}
```

* *total_number_of_words* - общее кол-во слов в тексте
* *total_number_of_word_pairs* - общее кол-во пар
* "Roman Empire"; предположим, что это уникальная комбинация, и за каждым появлением «Roman» следует «Empire», и, наоборот, каждому появлению «Empire» предшествует «Roman». В этом случае «P (ab) = P (a) = P(b)», поэтому «PMI (a, b) = -lnP(a) = -lnP(b)». Чем реже встречается эта коллокация, тем больше значение PMI.
* "the doors"; предположим, что «the» может встретится рядом с любым словом. Таким образом, «P (ab) = P (a) * P (b)» и «PMI (a, b) = ln 1 = 0".
* "green idea / sleeps furiously"; когда два слова никогда не встречаются вместе, «P (ab) = 0» и «PMI (a, b) = -inf».
 
NPMI вычисляется как $`NPMI(a,b)=-\frac{PMI(a,b)}{lnP(a,b)}`$ . Это нормализует величину в диапазон *[-1; 1]*.

Найти самые популярные коллокации в Википедии. Обработка данных:
* При парсинге отбрасывайте все символы, которые не являются латинскими буквами: `text = re.sub("^\W+|\W+$", "", text)`
* приводим все слова к нижнему регистру;
* удаляем все стоп-слова (даже внутри биграммы т.к. “at evening” имеет ту же семантику что и “at the evening”);
биграммы объединить символом нижнего подчеркивания "_";
* работаем только с теми биграммами, которые встретились не реже 500 раз (т.е. проводим все необходимые join'ы и считаем NPMI только для них).
* общее число слов и биграмм считать до фильтрации.
 
Для каждой биграммы посчитать NPMI и вывести на экран (в STDOUT) TOP-39 самых популярных коллокаций, отсортированных по убыванию значения NPMI. Само значение NPMI выводить не нужно.

Пример вывода
```
roman_empire
south_africa
```
Пример вывода на sample-датасете (со значениями NPMI):
```
19th_century	0.757464166177
20th_century	0.751460453349
references_external	0.731826941011
soviet_union	0.727806412183
air_force	0.705773204264
baseball_player	0.691711138551
university_press	0.687424532005
roman_catholic	0.683677693663
united_kingdom	0.68336461567
```
Подсказка: если вы все сделаете правильно, то «roman_empire» и «south_africa» будут в ответе.
##### Важно: Сначала нужно разделить слова по пробелу, и лишь потом убирать знаки препинания.

## Задача 2. Spark Streaming
#### Исходные данные
Входные данные: `/data/realtime/uids`

Формат выходных данных:
```
...
seg_firefox 4176
...
```

#### Условие

Сегмент - это множество пользователей, определяющееся неким признаком. Когда пользователь посещает web-сервис со своего устройства, это событие логируется на стороне web-сервиса в следующем формате: `user_id <tab> user_agent`. Например:
```
f78366c2cbed009e1febc060b832dbe4	Mozilla/5.0 (Linux; Android 4.4.2; T1-701u Build/HuaweiMediaPad) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.73 Safari/537.36
62af689829bd5def3d4ca35b10127bc5	Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36
```
На вход поступают порции web-логов в описанном формате. Требуется разбить аудиторию (пользователей) в этих логах на следующие сегменты:
1. Пользователи, которые работают в интернете из-под IPhone.
2. Пользователи, кот. используют Firefox браузер.
3. Пользователи, кот. используют Windows.

Не стоит волноваться если какие-то пользователи не попадут ни в 1 из указанных сегментов поскольку в реальной жизни часто попадаются данные, которые сложно классифицировать. Таких пользователей просто не включаем в выборку.

Также сегменты могут пересекаться (ведь возможен вариант, что пользователь использует Windows, на котором стоит Firefox). Для того, чтоб выделить сегменты можно использовать следующие эвристики (или придумать свои):

|Сегмент|Эвристика|
|----|----|
|seg_iphone|`parsed_ua['device']['family'] like '%iPhone%'`|
|seg_firefox|`parsed_ua['user_agent']['family'] like '%Firefox%'`|
|seg_windows|`parsed_ua['os']['family'] like '%Windows%'`|

Оцените кол-во уникальных пользователей в каждом сегменте используя алгоритм [HyperLogLog](https://github.com/svpcom/hyperloglog) (поставьте `error_rate` равным 1%).
В результате выведите сегменты и количества пользователей в следующем формате: `segment_name <tab> count`. Отсортируйте результат по количеству пользователей в порядке убывания.

#### Код для генерации батчей
В задаче используйте его без изменений т.к. он критичен для системы проверки.
```python
from hdfs import Config
import subprocess

client = Config().get_client()
nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")

sc = SparkContext(master='yarn-client')

# Preparing base RDD with the input data
DATA_PATH = "/data/realtime/uids"

batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)[:30]]

# Creating QueueStream to emulate realtime data generating
BATCH_TIMEOUT = 2 # Timeout between batch generation
ssc = StreamingContext(sc, BATCH_TIMEOUT)
dstream = ssc.queueStream(rdds=batches)
```
