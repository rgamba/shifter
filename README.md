# shift

Cassandra migration tool that uses pure CQL to keep the complexity away from migrations and from ORMs.

shift is based on the idea of simple pure CQL migration files than have a very specific order and should be included in your version control.

## Instalation

```bash
git clone https://github.com/rgamba/shift && python setup.py install
```

## Getting started


```bash
cqlmigrate --help
```