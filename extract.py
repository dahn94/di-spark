import json

import pandas as pd
import ray
import requests

ray.init()


@ray.remote
def tarefa1():
    start_date1 = "1994-07-04"
    end_date1 = "1997-11-21"
    daterange = pd.date_range(start=start_date1, end=end_date1)
    data_atual = start_date1
    list_json1 = []
    for i in daterange:
        if data_atual != str(i.strftime("%Y-%m-%d")):
            response = requests.get(
                f'https://calculadorarendafixa.com.br/calculadora/di/calculo?dataInicio={data_atual}&dataFim={str(i.strftime("%Y-%m-%d"))}&percentual=100.00&valor=1000.00')
            list_json1.append(response.json())

        data_atual = str(i.strftime("%Y-%m-%d"))

    return list_json1


@ray.remote
def tarefa2():
    start_date2 = "1997-11-21"
    end_date2 = "2001-04-10"
    daterange = pd.date_range(start=start_date2, end=end_date2)
    data_atual = start_date2
    list_json2 = []
    for i in daterange:
        if data_atual != str(i.strftime("%Y-%m-%d")):
            response = requests.get(
                f'https://calculadorarendafixa.com.br/calculadora/di/calculo?dataInicio={data_atual}&dataFim={str(i.strftime("%Y-%m-%d"))}&percentual=100.00&valor=1000.00')
            list_json2.append(response.json())

        data_atual = str(i.strftime("%Y-%m-%d"))

    return list_json2


@ray.remote
def tarefa3():
    start_date3 = "2001-04-10"
    end_date3 = "2004-08-28"
    daterange = pd.date_range(start=start_date3, end=end_date3)
    data_atual = start_date3
    list_json3 = []
    for i in daterange:
        if data_atual != str(i.strftime("%Y-%m-%d")):
            response = requests.get(
                f'https://calculadorarendafixa.com.br/calculadora/di/calculo?dataInicio={data_atual}&dataFim={str(i.strftime("%Y-%m-%d"))}&percentual=100.00&valor=1000.00')
            list_json3.append(response.json())

        data_atual = str(i.strftime("%Y-%m-%d"))

    return list_json3


@ray.remote
def tarefa4():
    start_date4 = "2004-08-28"
    end_date4 = "2008-01-16"
    daterange = pd.date_range(start=start_date4, end=end_date4)
    data_atual = start_date4
    list_json4 = []
    for i in daterange:
        if data_atual != str(i.strftime("%Y-%m-%d")):
            response = requests.get(
                f'https://calculadorarendafixa.com.br/calculadora/di/calculo?dataInicio={data_atual}&dataFim={str(i.strftime("%Y-%m-%d"))}&percentual=100.00&valor=1000.00')
            list_json4.append(response.json())

        data_atual = str(i.strftime("%Y-%m-%d"))

    return list_json4


@ray.remote
def tarefa5():
    start_date5 = "2008-01-16"
    end_date5 = "2011-06-04"
    daterange = pd.date_range(start=start_date5, end=end_date5)
    data_atual = start_date5
    list_json5 = []
    for i in daterange:
        if data_atual != str(i.strftime("%Y-%m-%d")):
            response = requests.get(
                f'https://calculadorarendafixa.com.br/calculadora/di/calculo?dataInicio={data_atual}&dataFim={str(i.strftime("%Y-%m-%d"))}&percentual=100.00&valor=1000.00')
            list_json5.append(response.json())

        data_atual = str(i.strftime("%Y-%m-%d"))

    return list_json5


@ray.remote
def tarefa6():
    start_date6 = "2011-06-04"
    end_date6 = "2014-10-22"
    daterange = pd.date_range(start=start_date6, end=end_date6)
    data_atual = start_date6
    list_json6 = []
    for i in daterange:
        if data_atual != str(i.strftime("%Y-%m-%d")):
            response = requests.get(
                f'https://calculadorarendafixa.com.br/calculadora/di/calculo?dataInicio={data_atual}&dataFim={str(i.strftime("%Y-%m-%d"))}&percentual=100.00&valor=1000.00')
            list_json6.append(response.json())

        data_atual = str(i.strftime("%Y-%m-%d"))

    return list_json6


@ray.remote
def tarefa7():
    start_date7 = "2014-10-22"
    end_date7 = "2018-03-12"
    daterange = pd.date_range(start=start_date7, end=end_date7)
    data_atual = start_date7
    list_json7 = []
    for i in daterange:
        if data_atual != str(i.strftime("%Y-%m-%d")):
            response = requests.get(
                f'https://calculadorarendafixa.com.br/calculadora/di/calculo?dataInicio={data_atual}&dataFim={str(i.strftime("%Y-%m-%d"))}&percentual=100.00&valor=1000.00')
            list_json7.append(response.json())

        data_atual = str(i.strftime("%Y-%m-%d"))

    return list_json7


@ray.remote
def tarefa8():
    start_date8 = "2018-03-12"
    end_date8 = "2021-07-26"
    daterange = pd.date_range(start=start_date8, end=end_date8)
    data_atual = start_date8
    list_json8 = []
    for i in daterange:
        if data_atual != str(i.strftime("%Y-%m-%d")):
            response = requests.get(
                f'https://calculadorarendafixa.com.br/calculadora/di/calculo?dataInicio={data_atual}&dataFim={str(i.strftime("%Y-%m-%d"))}&percentual=100.00&valor=1000.00')
            list_json8.append(response.json())

        data_atual = str(i.strftime("%Y-%m-%d"))

    return list_json8


# Execute func1 and func2 in parallel.
ret_id1 = tarefa1.remote()
ret_id2 = tarefa2.remote()
ret_id3 = tarefa3.remote()
ret_id4 = tarefa4.remote()
ret_id5 = tarefa5.remote()
ret_id6 = tarefa6.remote()
ret_id7 = tarefa7.remote()
ret_id8 = tarefa8.remote()

ret1, ret2, ret3, ret4, ret5, ret6, ret7, ret8 = ray.get([ret_id1, ret_id2, ret_id3, ret_id4, ret_id5, ret_id6, ret_id7, ret_id8])

data_json = (ret1 + ret2 + ret3 + ret4 + ret5 + ret6 + ret7 + ret8)


with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(data_json, f, ensure_ascii=False, indent=4)

df = pd.read_json('data.json')
df.to_csv('data_entrada.csv', encoding='utf-8', index=False)
