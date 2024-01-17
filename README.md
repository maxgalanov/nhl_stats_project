# NHL stats
В этом проекте мы реализовали ETL пайплайны от забора данных с API NHL stats до [telegram бота](https://t.me/nhl_bigdata_bot) и [дашбордов в Datalens](https://datalens.yandex/xqnhz02g6x6ml).
## Contributors
|| telegram |
| ------ | ------ |
| Maxim Galanov | [@maxglnv](https://t.me/maxglnv) |
| Shiryaeva Victoria | [@shiryaevva](https://t.me/shiryaevva) |
| Nikolay Boyarkin | [@zendeer](https://t.me/zendeer) |

## Stack
- Airflow для оркестрации ETL пайплайнов
- Hadoop + Spark для хранения и обработки данных
- DataLens и асинхронный Telegram bot в качестве UI интерфейса к данным
- PostgreSQL в качестве БД для DataLens
