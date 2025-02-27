from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
import logging
from tqdm import tqdm

# Настройка логирования для отслеживания процесса ETL
logging.basicConfig(level=logging.INFO)

# Определение путей к файлам
DATA_FILE = '/opt/airflow/data/profit_table.csv'
EXTRACTED_FILE = '/opt/airflow/data/extracted_data.csv'
TRANSFORMED_FILE = '/opt/airflow/data/transformed_data.csv'
OUTPUT_FILE = '/opt/airflow/data/flags_activity.csv'

# Функция для извлечения данных из CSV-файла
def extract():
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Файл {DATA_FILE} не найден!")

    logging.info("Извлечение данных из CSV...")
    df = pd.read_csv(DATA_FILE)
    logging.info(f"Данные успешно извлечены: {len(df)} строк.")

    df.to_csv(EXTRACTED_FILE, index=False)
    logging.info(f"Данные сохранены в {EXTRACTED_FILE}.")

# Функция для трансформации извлечённых данных
def transform_data():
    if not os.path.exists(EXTRACTED_FILE):
        raise FileNotFoundError(f"Файл {EXTRACTED_FILE} не найден!")

    logging.info(f"Чтение данных из {EXTRACTED_FILE}...")
    profit_table = pd.read_csv(EXTRACTED_FILE)

    logging.info("Начало трансформации данных...")

    # Вложенная функция для выполнения трансформации
    def transform(profit_table, date):
        start_date = pd.to_datetime(date) - pd.DateOffset(months=2)
        end_date = pd.to_datetime(date) + pd.DateOffset(months=1)
        date_list = pd.date_range(start=start_date, end=end_date, freq='M').strftime('%Y-%m-01')

        # Группировка данных по 'id' и суммирование значений
        df_tmp = (
            profit_table[profit_table['date'].isin(date_list)]
            .drop('date', axis=1)
            .groupby('id')
            .sum()
        )

        # Создание флагов для каждого продукта
        product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
        for product in tqdm(product_list):
            df_tmp[f'flag_{product}'] = (
                df_tmp.apply(
                    lambda x: int(x[f'sum_{product}'] != 0 and x[f'count_{product}'] != 0),
                    axis=1
                )
            )

        # Фильтрация только нужных столбцов с флагами
        return df_tmp.filter(regex='flag').reset_index()

    transformed_df = transform(profit_table, '2024-03-01')
    logging.info("Трансформация завершена.")

    transformed_df.to_csv(TRANSFORMED_FILE, index=False)
    logging.info(f"Трансформированные данные сохранены в {TRANSFORMED_FILE}.")

# Функция для загрузки трансформированных данных в CSV
def load():
    if not os.path.exists(TRANSFORMED_FILE):
        raise FileNotFoundError(f"Файл {TRANSFORMED_FILE} не найден!")

    logging.info(f"Чтение трансформированных данных из {TRANSFORMED_FILE}...")
    df = pd.read_csv(TRANSFORMED_FILE)

    logging.info("Запись данных в выходной CSV...")
    file_exists = os.path.isfile(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, mode='a', header=not file_exists, index=False)
    logging.info(f"Данные успешно записаны в {OUTPUT_FILE}.")

# Определяем DAG для ETL процесса
with DAG(
    'Babenko_maxim',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 10, 5),
        'retries': 1,
    },
    schedule_interval='@monthly',  # Запускать каждый месяц
    catchup=False,
) as dag:

    # Создание задач для DAG
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
    )

    # Определение последовательности выполнения задач
    extract_task >> transform_task >> load_task
