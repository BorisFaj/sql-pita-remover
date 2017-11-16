#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import json
import argparse
import pandas as pd

parser = argparse.ArgumentParser(description='Crea los ficheros de mapping en formato json a partir de un excel')

parser.add_argument('--excel', dest='excel_path', required=True, type=str,
                    help='Ruta al fichero excel que contiene el mapeo en crudo')
parser.add_argument('--conf', dest='conf_path', required=False, default=os.path.join('conf', 'mapping'), type=str,
                    help='Ruta a la carpeta de configuracion donde se van a almacenar los json con los mapeos')
parser.add_argument('--sep', dest='sep', required=False, default=';', type=str,
                    help='Caracter que utilizado como separador cuando se hace referencia a mas de una tabla')


# Funciones
def write_json(path, d):
    """Escribe un diccionario en un fichero json en el directorio especificado.

    Parameters
    ----------
    path: str
        Directorio de salida del json.
    d: dict
        Diccionario que se va a persistir.

    """
    file_path = os.path.join(path, str(d['old_name']).upper() + '.json')
    with open(file_path, 'w') as fp:
        json.dump(d, fp, indent=4)


def __init_dict__(e):
    """Inicializa un json. El formato es el de mapeo de una tabla nueva.

    Parameters
    ----------
    e: pd.Series
        Fila del excel que se esta procesando.

    Returns
    -------
    dict
        Diccionario nuevo.
    """
    return {'old_name': e['Tabla Origen'],
            'new_name': e['Tabla'],
            'fields': {}
            }


def _generate_json(e, all_json):
    """Actualiza el json correspondiente a la fila que se esta procesando.

    Parameters
    ----------
    e: pd.Series
        Fila del excel que se esta procesando.
    all_json: dict
        Diccionario que contiene todos los json de todas las tablas.
    """
    try:
        table = all_json.get(e['Tabla Origen'], __init_dict__(e))
        table['fields'].setdefault(e['columnaLegacy'].upper(), e['Code'])

        all_json[e['Tabla Origen']] = table

    except KeyError as err:
        raise KeyError('Error accediendo al diccionario. Por favor, comprobar que los nombres de las columnas del excel'
                       'no han cambiado.\n{}'.format(err))


def __process_table__(table, e, all_jsons, index):
    """Procesa una sola tabla, asignandole los campos correspondientes.

    Parameters
    ----------
    table: str
        Nombre de la tabla a procesar.
    e: pd.Series
        Fila del excel que se esta procesando.
    all_jsons: dict
        Diccionario que contiene todo los json de todas las tablas.
    index: list(str)
        Lista con los indices originales de la fila.
    """
    new = pd.Series(table).append(e.iloc[1:])
    new.index = index
    _ = _generate_json(new, all_jsons)


def process_row(e, all_jsons, sep):
    """Procesa una fila del excel. Separando en diferentes json cuando una fila hace referencia a mas de una tabla,
    para al final generar un json por cada tabla.

    Parameters
    ----------
    e: pd.Series
        Fila del excel que se esta procesando.
    all_jsons: dict
        Diccionario que contiene todos los json de todas las tablas.
    sep: str
        Caracter que separa los nombres de las tablas, cuando hay mas de una en esa fila.
    """
    original_index = e.index
    [__process_table__(table, e, all_jsons, original_index) for table in e[0].split(sep)]


def process_all(excel_path, sep, conf_path):
    """Ejecuta todo el proceso de parseo de excels y persiste los datos procesados en ficheros json en la ruta indicada
    por paramerto.

    Parameters
    ----------
    excel_path: str
        Ruta al fichero excel a procesar.
    sep: str
        Caracter que separa los nombres de las tablas, cuando hay mas de una en esa fila.
    conf_path: str
        Ruta a la carpeta de configuracion donde se van a almacenar los json.
    """
    # Cargar el excel
    raw = pd.read_excel(excel_path)
    df = raw.dropna(subset=[raw.columns[0]])
    logging.info('De las {} filas originales, solo {} van informadas en la primera columna'
                 .format(raw.shape[0], df.shape[0]))

    # Procesar el excel
    all_json = {}
    df.apply(lambda x: process_row(x, all_json, sep), axis=1)

    # Persistir los json
    for conf_file in all_json.keys():
        all_json[conf_file]['fields'].setdefault('*', '*')
        write_json(conf_path, all_json[conf_file])

    logging.info('Procesamiento finalizado, los json con los mapeos estan guardados en {}'.format(conf_path))


if __name__ == "__main__":
    # Leer los parametros
    conf = parser.parse_args()
    # Procesar
    process_all(conf.excel_path, conf.sep, conf.conf_path)
