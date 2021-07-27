# di-spark
Extração e processamento dos dados referentes a série histórica do DI desde 04/07/1994 à 26/07/2021

<h2> Configuração necessária para rodar o projeto</h2>

1. Clone o repositório contendo imagens dockers prontas para rodar o projeto:
```
$ git clone https://github.com/jupyter/docker-stacks.git
```

2. Execute a seguinte imagem:
```
$ cd docker-stacks
$ docker run -p 8888:8888 jupyter/pyspark-notebook
```

3. No terminal do container fazer o clone do projeto:
```
$ git clone https://github.com/dahn94/di-spark.git
```

4. Instale algumas dependencias:
```
$ pip install -r requirements.txt
```

5. Realize a extração dos dados: 
```
$ python3 extract.py
```

6. Realize o processamento dos dados e salve em formato data-table. 
```
$ python3 spark_processing.py
```
