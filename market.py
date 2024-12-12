import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список товаров в каталоге

    Args:
        page (str): идентификатор страницы c результатами
        campaign_id (int): идентификатор компании
        access_token (str): токен API Яндекс Маркет

    Return:
        dict: словарь со статусом ответа и списком товаров

    Example:
        >>> page = 'eyBuZXh0SWQ6IDIzNDIgfQ=='
        >>> campaign_id = 5325355
        >>> access_token = 'ACMA:I4c4CxCSYaI41RSC2uYWP2qj3RK:151c0664'
        >>> get_product_list(page, campaign_id, access_token)
        {
            "status": "OK",
            "result": {
                "paging": {
                    "nextPageToken": "string",
                    "prevPageToken": "string"
                },
                "offerMappings": [
                    {
                        "offer": {
                            "offerId": "string",
                            "name": "Ударная дрель Makita HP1630, 710 Вт",
                            "marketCategoryId": 0,
                            "category": "string",
                            "pictures": [
                                "string"
                            ],
                            ...
                    }
                ]
            }
        }
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет количество остатков товаров

    Args:
        stocks(list): список с информацией о остатках товаров
        (вывод функции create_stocks)
        campaign_id (int): идентификатор компании
        access_token (str): токен API Яндекс Маркет

    Return:
        dict: словарь с данными ответа сервера

    Example:
        >>> stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
        >>> campaign_id = 5325355
        >>> access_token = 'ACMA:I4c4CxCSYaI41RSC2uYWP2qj3RK:151c0664'
        >>> update_stocks(stocks, campaign_id, access_token)
        {
            "skus": [
                {
                    "sku": "string",
                    "warehouseId": 0,
                    "items": [
                        {
                            "count": 0,
                            "type": "FIT",
                            "updatedAt": "2022-12-29T18:02:01Z"
                        }
                    ]
                },
                ...
            ]
        }
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет цены товаров

    Args:
        prices(list): список словарей с ценами и артикулами товаров
        campaign_id (int): идентификатор компании
        access_token (str): токен API Яндекс Маркет

    Return:
        dict: словарь с данными ответа сервера

    Example:
        >>> stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
        >>> campaign_id = 5325355
        >>> access_token = 'ACMA:I4c4CxCSYaI41RSC2uYWP2qj3RK:151c0664'
        >>> update_price(prices, campaign_id, access_token)
        {
         "status": "OK"
        }
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получает артикулы товаров Яндекс маркета

    Args:
        campaign_id (int): идентификатор компании
        market_token (str): токен API Яндекс Маркет

    Return:
        list: список артикулов товаров

    Example:
        >>> campaign_id = 5325355
        >>> market_token = 'ACMA:I4c4CxCSYaI41RSC2uYWP2qj3RK:151c0664'
        >>> get_offer_ids(campaign_id, market_token)
        ['4VRS5962338D', 'KJ-3598/4313', ...]
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Создаёт список остатков товара

    Количество остатков рассчитывается следующим образом:
    остатки|вывод
    >10    | 100
    1      | 0
    другое | это же
    число  | число

    Если в offer_ids есть товар, которого нет в watch_remnants, то
    количество остатков будет помечено как "0"

    Args:
        watch_remnants (list): список словарей с данными о часах
        (вывод функции download_stock)
        offer_ids (list): список артикулов товаров
        (вывод функции get_offer_ids)
        warehouse_id (int): Идентификатор склада

    Return:
        list: список словарей с информацией об остатках товаров

    Example:
        >>> watch_remnants = download_stock()
        >>> offer_ids = get_offer_ids(campaign_id, market_token)
        >>> warehouse_id = 4244
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        [
            {
                "sku": '4VRS5962338D',
                "warehouseId": 4244,
                "items": [
                    {
                        "count": 1,
                        "type": "FIT",
                        "updatedAt": "2024-12-11T14:30:45Z",
                    }
                ],
            },
            ...
        ]
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создаёт список цен товаров

    Args:
        watch_remnants (list): список словарей с данными о часах
        (вывод функции download_stock)
        offer_ids (list): список артикулов товаров
        (вывод функции get_offer_ids)

    Return:
        list: список словарей с информацией о ценах товаров

    Example:
        >>> watch_remnants = download_stock()
        >>> offer_ids = get_offer_ids(campaign_id, market_token)
        >>> create_prices(watch_remnants, offer_ids)
        [
            {
                'id': '71108',
                'price': {
                    'value': '6490',
                    'currencyId': 'RUR'
                }
            },
            ...
        ]

    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Асинхронно обновляет цены товаров на Яндекс Маркете

    Использует вспомогательные функции для получения артикулов, получения
    списка цен и для отправки данных частями в соответствии с
    ограничениями API

    Args:
        watch_remnants (list): список словарей с данными о часах
        (вывод функции download_stock)
        campaign_id (int): идентификатор компании
        market_token (str): токен API Яндекс Маркет

    Return:
        list: список словарей с информацией о ценах товаров

    Example:
        >>> watch_remnants = download_stock()
        >>> campaign_id = 5325355
        >>> market_token = 'ACMA:I4c4CxCSYaI41RSC2uYWP2qj3RK:151c0664'
        >>> upload_prices(watch_remnants, campaign_id, market_token)
        [
            {
                'id': '71108',
                'price': {
                    'value': '6490',
                    'currencyId': 'RUR'
                }
            },
            ...
        ]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Асинхронно обновляет остатки товаров на Яндекс Маркете

    Использует вспомогательные функции для получения артикулов, получения
    списка остатков и для отправки данных частями в соответствии с
    ограничениями API

    Args:
        watch_remnants (list): список словарей с данными о часах
        (вывод функции download_stock)
        campaign_id (int): идентификатор компании
        market_token (str): токен API Яндекс Маркет
        warehouse_id (int): Идентификатор склада

    Return:
        not_empty (list): список остатков товаров с их наличием
        stocks (list): список всех остатков товаров

    Example:
        >>> watch_remnants = download_stock()
        >>> campaign_id = 5325355
        >>> market_token = 'ACMA:I4c4CxCSYaI41RSC2uYWP2qj3RK:151c0664'
        >>> warehouse_id = 4244
        >>> non_empty, stocks = upload_stocks(
        ...     watch_remnants, campaign_id, market_token, warehouse_id)
        >>> non_empty
        [
            {
                "sku": '4VRS5962338D',
                "warehouseId": 4244,
                "items": [
                    {
                        "count": 10,
                        "type": "FIT",
                        "updatedAt": "2024-12-11T14:30:45Z",
                    }
                ],
            },
            ...
        ]
        >>> stocks
                [
            {
                "sku": 'KJ-3598/4313',
                "warehouseId": 4244,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": "2024-12-11T14:30:45Z",
                    }
                ],
            },
            ...
        ]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
