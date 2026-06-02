# 2transport-datavault-etl

ETL пайплайн для сбора, обработки и загрузки данных о ДТП в Data Vault на PostgreSQL.

## Архитектура
- Data Vault 2.0
- Apache Airflow для оркестрации
- PostgreSQL в Docker
- Pandas для трансформации

## Быстрый старт

```bash
# Клонирование
git clone https://github.com/Ilyaradaev/2transport-datavault-etl.git
cd 2transport-datavault-etl

# Запуск Docker
docker-compose up -d

# Установка зависимостей
pip install -r requirements.txt

# Запуск ETL
python dags/2TRANSPORTv2.py
