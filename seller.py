import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров магазина озон.

    С помощью API Ozon получает словарь со списком товаров, установлено
    ограничение в 1000 товаров, поэтому нужен параметр last_id, чтобы отсчёт
    начинать с этого товара. в словаре помимо списка есть ключ total с
    количеством товаров и last_id.

    Args:
        last_id (str):  id товара для начала отсчёта
        client_id (str): идентификатор клиента
        seller_id (str): ключ продавца
    
    Return:
        dict: словарь со списком товаров магазина, общее кол-во и id
        последнего товара
    
    Example:
        >>> get_product('48234', '312345', 'qwe68324')
        {
            items : [
                {"product_id": 1234124, "offer_id": "51221312", ...},
                {"product_id": 321321, "offer_id": "74353245", ...},
                ...
            ],
            "total": 1000,
            "last_id": 5142132
        }
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает артикулы товаров магазина озон

    get_offer_ids использует в цикле функцию get_product_list и бесконечно
    итерируется пока не добавит все артикулы в список

    Args:
        client_id (str) идентификатор клиента
        seller_id (str) ключ продавца

    Return:
        list: Список артикулов товаров

    Example:
        >>> get_offer_ids('312345', 'qwe68324')
        ['51221312', '74353245', ...]
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров в магазине с помощью API Ozon

    Args:
        prices(list): список словарей с ценами и артикулами для изменения
        текущих цен на них (вывод функции create_prices)
        client_id (str): идентификатор клиента
        seller_id (str): ключ продавца

    Return:
        dict: ответ сервера в виде словаря с информацией о успешности
        или ошибками при запросе

    Example:
        >>> prices = create_prices(watch_remnants, offer_ids)
        >>> update_price(prices, '312345', 'qwe68324')
        {
            result : {
                Updated: [
                    {"offer_id": "51221312", "status": "success"},
                    {"offer_id": "74353245", "status": "success"}
                ],
                errors: []
            }
        }
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров в магазине с помощью API Ozon

    Args:
        stocks(list): список словарей с остатками и артикулами
        (вывод функции create_stocks)
        client_id (str): идентификатор клиента
        seller_id (str): ключ продавца

    Return:
        dict: ответ сервера в виде словаря с информацией о успешности
        или ошибками при запросе

    Example:
        >>> stocks = create_stocks(watch_remnants, offer_ids)
        >>> update_stocks(stocks list, '312345', 'qwe68324')
        {
            result : {
                Updated: [
                    {"offer_id": "51221312", "status": "success"},
                    {"offer_id": "74353245", "status": "success"}
                ],
                errors: []
            }
        }
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает файл ostatki с сайта casio и считывает оттуда данные.

    Скачивает .xls файл с сайта, считывает данные о часах, удаляет скачанный
    файл и возвращает список словарей с этими данными.

    Args:
        None

    Return:
        list: список словарей с данными о часах

    Example:
        >>> download_stock()
        [
            {"Код": "123124", "Наименование товара": "BA-110AQ-4A", ...},
            {"Код": "51353", ...},
            ...
        ]
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Получает список словарей остатков товара

    Количество остатков расчитывается следующим образом:
    остатки|вывод
    >10    | 100
    1      | 0
    другое | это же
    число  | число

    Если в offer_ids есть товар, которого нет в watch_remnants, то
    количество остатков будет помечено как "0"

    Args:
        watch_remnants(list): список словарей с данными о часах
        (вывод функции download_stock)
        offer_ids(list): список артикулов товаров магазина
        (вывод функции get_offer_ids)

    Return:
        list: список словарей формата артикул: остаток

    Example:
        >>> offer_ids = get_offer_ids('312345', 'qwe68324')
        >>> watch_remnants = download_stock()
        >>> create_stocks(watch_remnants, offer_ids)
        [{'4124124': '6'}, {'3214124}: '100'}, ...]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создаёт список словарей с ценами товаров

    Args:
        watch_remnants(list): список словарей с данными о часах
        (вывод функции download_stock)
        offer_ids(list): список артикулов товаров магазина
        (вывод функции get_offer_ids)

    Return:
        list: список словарей с информацией о цене товара

    Example:
        >>> offer_ids = get_offer_ids('312345', 'qwe68324')
        >>> watch_remnants = download_stock()
        >>> create_prices(watch_remnants, offer_ids)
        [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "1234123",
                "old_price": "0", "price":
                "26'990.00 руб."
            },
            {...},
            ...
        ]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовывает цену, возвращает отформатированную строку цены.

    Args:
        price (str): цена продукта

    Returns:
       str: строка цены без специальных символов и валюты

    Example:
        >>> price_conversion("5'990.00 руб.")
        "5990"
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список lst на части по n элементов

    Необходимо для приспособления к огранечениям API
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
