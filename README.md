# SQLFuzz: Random SQLAlchemy Query Generator

`sqlfuzz` is a random SQLAlchemy query generator for DBMS fuzzing toolchains. `sqlfuzz` improves upon other random query generators, such as `sqlsmith`, by leveraging SQLAlchemy's ability to compile the same query for different DBMS dialects. For example, `sqlfuzz` allows us to easily test the queries that triggers bugs in Postgres to other DBMSs such as MySQL, MariaDB, Sqlite, etc. `sqlfuzz` can target [any dialects that SQLAlchemy supports](https://docs.sqlalchemy.org/en/20/dialects/).

## Install

```
$ pip3 install sqlfuzz
```

## Usage

### Quick Start

```
import json
from sqlfuzz.fuzz import Fuzz

f = open("./path/to/probability/conf.json")
prob_conf = json.load(f)
connection_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
fuzz = Fuzz(prob_conf, connection_string)

queries = fuzz.gen_orm_queries(count=10)
```

### Probability Configuration

Probability of the fuzzer chosing to generate a query with following clauses (range: 0-1000)

```
{
  'order': 500,
  'limit': 500,
  'group': 500,
  'left': 500,
  'inner': 500,
  'full': 500,
  'scalar': 500,
  'true': 500,
  'func_expr': 500,
  'literal_column': 500,
  'distinct': 500,
  'set': 500,
  'offset': 500,
  'simple': 500,
  'window': 500,
  'extractyear': 500,
  'extractmonth': 500,
  'subquery': 500,
  'nested': 500
}
```
