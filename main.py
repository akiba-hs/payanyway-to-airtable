import os
import hashlib
import logging
from typing import Dict, Union

from fastapi import FastAPI, Request, Response, HTTPException
import httpx
from dotenv import load_dotenv

# Загрузка .env при локальном запуске (необязательно в продакшене)
load_dotenv()

# Получаем переменные окружения
MNT_ID = os.getenv("MNT_ID", "").strip()
INTEGRITY_CODE = os.getenv("MNT_INTEGRITY_CODE", "").strip()
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Payments").strip()

if not all([MNT_ID, INTEGRITY_CODE, AIRTABLE_API_KEY, AIRTABLE_BASE_ID]):
    logging.critical("Не заданы обязательные переменные окружения MNT_ID, INTEGRITY_CODE, AIRTABLE_API_KEY, AIRTABLE_BASE_ID")

app = FastAPI()


def calculate_signature(params: Dict[str, str]) -> str:
    """
    Вычисляет MD5-подпись по формуле:
    MD5(MNT_ID + MNT_TRANSACTION_ID + MNT_OPERATION_ID + MNT_AMOUNT +
        MNT_CURRENCY_CODE + MNT_SUBSCRIBER_ID + MNT_TEST_MODE + INTEGRITY_CODE)
    МNT_AMOUNT форматируется с двумя знаками после точки через точку.
    """
    # Извлекаем параметры, которые должны присутствовать
    mnt_trx_id     = params.get("MNT_TRANSACTION_ID", "")
    mnt_op_id      = params.get("MNT_OPERATION_ID", "")
    mnt_amount_raw = params.get("MNT_AMOUNT", "")
    mnt_currency   = params.get("MNT_CURRENCY_CODE", "")
    mnt_subscriber = params.get("MNT_SUBSCRIBER_ID", "")
    mnt_test_mode  = params.get("MNT_TEST_MODE", "0")

    # Собираем строку для хэширования
    data_to_sign = (
        MNT_ID
        + mnt_trx_id
        + mnt_op_id
        + mnt_amount_raw
        + mnt_currency
        + mnt_subscriber
        + mnt_test_mode
        + INTEGRITY_CODE
    )
    logging.info(data_to_sign)
    # Возвращаем 32-символьную hex-строку в нижнем регистре
    return hashlib.md5(data_to_sign.encode("utf-8")).hexdigest()


async def update_airtable_record(record_id: str, amount: Union[float, str], status: str) -> None:
    """
    Асинхронно обновляет запись в Airtable:
    PATCH https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_id}
    Поля: Amount, Status
    """
    airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}/{record_id}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "fields": {
            "Amount": amount,
            "Status": status
        }
    }

    async with httpx.AsyncClient() as client:
        response = await client.patch(airtable_url, headers=headers, json=payload, timeout=10.0)

    if response.status_code not in (200, 201):
        # Логируем подробно текст ошибки Airtable
        logging.error(
            "Ошибка при обновлении Airtable (record_id=%s): %s %s",
            record_id,
            response.status_code,
            response.text
        )
        raise HTTPException(status_code=500, detail="Airtable update failed")


@app.api_route("/webhook", methods=["GET", "POST"])
async def moneta_webhook(request: Request) -> Response:
    """
    Обработчик входящих уведомлений от Moneta.ru.
    Поддерживает как GET, так и POST (формат application/x-www-form-urlencoded или JSON).
    Шаги:
    1. Собираем все параметры из query и из body.
    2. Проверяем, что MNT_ID совпадает с ожидаемым.
    3. Вычисляем MD5-подпись и сверяем с MNT_SIGNATURE.
    4. Вычисляем статус: "Paid" или "Test Paid".
    5. Вызываем update_airtable_record для записи в Airtable.
    6. Возвращаем plain-текст "SUCCESS" с кодом 200 или "FAIL" + 400/500.
    """
    # 1. Сбор параметров
    params: Dict[str, str] = {}

    # 1.1. Параметры из query string
    for key, value in request.query_params.items():
        params[key] = value

    # 1.2. Параметры из формы (POST x-www-form-urlencoded)
    if request.headers.get("content-type", "").startswith("application/x-www-form-urlencoded"):
        form_data = await request.form()
        for key, value in form_data.items():
            params[key] = value

    # 1.3. Параметры из JSON-тела (POST application/json)
    if request.headers.get("content-type", "").startswith("application/json"):
        try:
            json_data = await request.json()
            if isinstance(json_data, dict):
                for key, value in json_data.items():
                    # преобразуем всё в строки
                    params[key] = str(value)
        except Exception:
            pass

    # Необходимые поля для проверки подписи
    mnt_id            = params.get("MNT_ID", "")
    mnt_trx_id        = params.get("MNT_TRANSACTION_ID", "")
    mnt_signature_rx  = params.get("MNT_SIGNATURE", "").lower()

    # 2. Проверяем идентификатор магазина
    if mnt_id != MNT_ID:
        logging.error("Неверный MNT_ID: получили %s, ожидали %s", mnt_id, MNT_ID)
        return Response(content="FAIL", status_code=400, media_type="text/plain")

    # 3. Проверяем подпись
    calc_sig = calculate_signature(params)
    if calc_sig != mnt_signature_rx:
        logging.error(
            "Несовпадение подписи: вычислено %s, получили %s", calc_sig, mnt_signature_rx
        )
        return Response(content="FAIL", status_code=400, media_type="text/plain")

    # 4. Определяем статус оплаты
    mnt_test_mode = params.get("MNT_TEST_MODE", "0")
    status_value = "Test Paid" if mnt_test_mode == "1" else "Paid"

    # 5. Формируем сумму, чтобы передать в Airtable
    # В calculate_signature мы уже форматировали сумму, но здесь получим из params
    mnt_amount_raw = params.get("MNT_AMOUNT", "0")

    # 6. Обновляем запись в Airtable
    try:
        await update_airtable_record(record_id=mnt_trx_id, amount=mnt_amount_raw, status=status_value)
    except HTTPException as e:
        # Если при обновлении Airtable произошла ошибка
        return Response(content="FAIL", status_code=e.status_code, media_type="text/plain")
    except Exception as e:
        logging.exception("Неожиданная ошибка при обновлении Airtable")
        return Response(content="FAIL", status_code=500, media_type="text/plain")

    # 7. Успешный ответ Moneta.ru
    return Response(content="SUCCESS", status_code=200, media_type="text/plain")
