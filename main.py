import os
import hashlib
import logging
from typing import Dict, List, Tuple, Union

from fastapi import FastAPI, Request, Response, HTTPException
import httpx
from xml.etree import ElementTree as ET
from xml.dom import minidom
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

MNT_ID = os.getenv("MNT_ID", "").strip()
INTEGRITY_CODE = os.getenv("MNT_INTEGRITY_CODE", "").strip()
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Payments").strip()

if not all([MNT_ID, INTEGRITY_CODE, AIRTABLE_API_KEY, AIRTABLE_BASE_ID]):
    logging.critical(
        "Не заданы обязательные переменные окружения: "
        "MNT_ID, MNT_INTEGRITY_CODE, AIRTABLE_API_KEY, AIRTABLE_BASE_ID"
    )

app = FastAPI()


def calculate_signature(params: Dict[str, str]) -> str:
    """
    Вычисляем MD5-подпись входящего запроса Moneta.ru:
    MD5(MNT_ID + MNT_TRANSACTION_ID + MNT_OPERATION_ID +
        MNT_AMOUNT + MNT_CURRENCY_CODE + MNT_SUBSCRIBER_ID +
        MNT_TEST_MODE + INTEGRITY_CODE)

    Здесь считаем, что params["MNT_AMOUNT"] уже строка с нужным форматом.
    """
    mnt_trx_id     = params.get("MNT_TRANSACTION_ID", "")
    mnt_op_id      = params.get("MNT_OPERATION_ID", "")
    mnt_amount_str = params.get("MNT_AMOUNT", "")          # Используем как есть
    mnt_currency   = params.get("MNT_CURRENCY_CODE", "")
    mnt_subscriber = params.get("MNT_SUBSCRIBER_ID", "")
    mnt_test_mode  = params.get("MNT_TEST_MODE", "0")

    data_to_sign = (
        MNT_ID
        + mnt_trx_id
        + mnt_op_id
        + mnt_amount_str
        + mnt_currency
        + mnt_subscriber
        + mnt_test_mode
        + INTEGRITY_CODE
    )
    return hashlib.md5(data_to_sign.encode("utf-8")).hexdigest()


async def update_airtable_record(record_id: str, amount: Union[str, float], status: str) -> None:
    """
    Обновляем запись в Airtable: PATCH https://api.airtable.com/v0/{BASE_ID}/{TABLE}/{record_id}
    Передаём "Amount" (в формате строки или float) и "Status".
    """
    airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}/{record_id}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }

    # Если amount — строка, оставляем как есть, Airtable примет строку или float
    payload = {
        "typecast": True,
        "fields": {
            "Amount": amount,
            "Status": status
        }
    }

    async with httpx.AsyncClient() as client:
        resp = await client.patch(airtable_url, headers=headers, json=payload, timeout=10.0)

    if resp.status_code not in (200, 201):
        logging.error(
            "Ошибка обновления Airtable (ID=%s): %s %s",
            record_id, resp.status_code, resp.text
        )
        raise HTTPException(status_code=500, detail="Airtable update failed")


def build_xml_response(
    mnt_id: str,
    mnt_trx_id: str,
    result_code: str,
    description: str,
    amount: str = "",
    attributes: List[Tuple[str, str]] = None
) -> str:
    """
    Собираем XML-ответ для Moneta.ru:
    <?xml version="1.0" encoding="UTF-8"?>
    <MNT_RESPONSE>
      <MNT_ID>...</MNT_ID>
      <MNT_TRANSACTION_ID>...</MNT_TRANSACTION_ID>
      <MNT_RESULT_CODE>...</MNT_RESULT_CODE>
      <MNT_DESCRIPTION>...</MNT_DESCRIPTION>
      <MNT_AMOUNT>...</MNT_AMOUNT>            # по условию, берем amount как строку
      <MNT_SIGNATURE>...</MNT_SIGNATURE>
      <MNT_ATTRIBUTES>
        <ATTRIBUTE>
          <KEY>...</KEY>
          <VALUE>...</VALUE>
        </ATTRIBUTE>
        ...
      </MNT_ATTRIBUTES>
    </MNT_RESPONSE>
    """
    root = ET.Element("MNT_RESPONSE")
    ET.SubElement(root, "MNT_ID").text = mnt_id
    ET.SubElement(root, "MNT_TRANSACTION_ID").text = mnt_trx_id
    ET.SubElement(root, "MNT_RESULT_CODE").text = result_code
    ET.SubElement(root, "MNT_DESCRIPTION").text = description

    # Если передали непустую строку amount, вставляем её напрямую
    if amount:
        ET.SubElement(root, "MNT_AMOUNT").text = amount

    # Формируем подпись: MD5(MNT_RESULT_CODE + MNT_ID + MNT_TRANSACTION_ID + INTEGRITY_CODE)
    sign_src = result_code + mnt_id + mnt_trx_id + INTEGRITY_CODE
    mnt_sig = hashlib.md5(sign_src.encode("utf-8")).hexdigest()
    ET.SubElement(root, "MNT_SIGNATURE").text = mnt_sig

    attrs_elem = ET.SubElement(root, "MNT_ATTRIBUTES")
    if attributes:
        for key, value in attributes:
            attr = ET.SubElement(attrs_elem, "ATTRIBUTE")
            ET.SubElement(attr, "KEY").text = key
            ET.SubElement(attr, "VALUE").text = value
    # Если attributes=None или пустой список, оставляем тег <MNT_ATTRIBUTES/> пустым

    # Преобразуем в красиво отформатированную XML-строку
    rough = ET.tostring(root, encoding="utf-8")
    parsed = minidom.parseString(rough)
    pretty_bytes = parsed.toxml(encoding="utf-8")
    return pretty_bytes.decode("utf-8")


@app.api_route("/webhook", methods=["GET", "POST"])
async def moneta_webhook(request: Request) -> Response:
    """
    1) Если нет ни одного параметра — отвечаем 200 OK как health check, content-type text/plain.
    2) Иначе:
       a) Собираем все параметры (query, form-data, JSON).
       b) Проверяем MNT_ID и MD5 подпись.
       c) Обновляем Airtable.
       d) Отправляем XML-ответ с нужным MNT_RESULT_CODE.
    """
    # 1) Проверка на отсутствие любых входных параметров
    #    (ни в query, ни в form, ни в JSON)
    has_query = len(request.query_params) > 0
    has_form = False
    has_json = False

    # Определим, есть ли form-data или JSON в теле
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/x-www-form-urlencoded"):
        form_data = await request.form()
        has_form = len(form_data) > 0
    elif content_type.startswith("application/json"):
        try:
            json_body = await request.json()
            has_json = isinstance(json_body, dict) and len(json_body) > 0
        except Exception:
            has_json = False

    if not (has_query or has_form or has_json):
        # Никаких параметров — это health check
        return Response(status_code=200, content="OK", media_type="text/plain")

    # 2) Собираем параметры в единый словарь строковых значений
    params: Dict[str, str] = {}
    # 2.1. Query-параметры
    for k, v in request.query_params.items():
        params[k] = v
    # 2.2. Form-data (если есть)
    if content_type.startswith("application/x-www-form-urlencoded"):
        for k, v in form_data.items():  # form_data определили выше
            params[k] = v
    # 2.3. JSON-тело (если есть)
    if content_type.startswith("application/json"):
        for k, v in json_body.items():  # json_body определили выше
            params[k] = str(v)

    # 3) Извлекаем ключевые поля
    mnt_id_rx        = params.get("MNT_ID", "")
    mnt_trx_id_rx    = params.get("MNT_TRANSACTION_ID", "")
    mnt_signature_rx = params.get("MNT_SIGNATURE", "").lower()
    mnt_amount_rx    = params.get("MNT_AMOUNT", "")
    mnt_test_mode_rx = params.get("MNT_TEST_MODE", "0")

    # 4) Проверяем MNT_ID
    if mnt_id_rx != MNT_ID:
        xml = build_xml_response(
            mnt_id=mnt_id_rx or "",
            mnt_trx_id=mnt_trx_id_rx or "",
            result_code="500",
            description="Invalid MNT_ID",
            amount=mnt_amount_rx,
            attributes=None
        )
        return Response(content=xml, status_code=200, media_type="application/xml")

    # 5) Проверка MD5-подписи
    calc_sig = calculate_signature(params)
    if calc_sig != mnt_signature_rx:
        xml = build_xml_response(
            mnt_id=MNT_ID,
            mnt_trx_id=mnt_trx_id_rx or "",
            result_code="500",
            description="Signature mismatch",
            amount=mnt_amount_rx,
            attributes=None
        )
        return Response(content=xml, status_code=200, media_type="application/xml")

    # 6) Определяем статус для Airtable
    status_value = "Test Paid" if mnt_test_mode_rx == "1" else "Paid"

    # 7) Обновляем Airtable
    try:
        # Поскольку сумма уже от магазина правильно отформатирована (строка с двумя знаками),
        # передаём её напрямую
        await update_airtable_record(
            record_id=mnt_trx_id_rx,
            amount=mnt_amount_rx,
            status=status_value
        )
    except HTTPException as e:
        xml = build_xml_response(
            mnt_id=MNT_ID,
            mnt_trx_id=mnt_trx_id_rx,
            result_code="500",
            description="Airtable update failed",
            amount=mnt_amount_rx,
            attributes=None
        )
        return Response(content=xml, status_code=200, media_type="application/xml")
    except Exception:
        logging.exception("Неожиданная ошибка при обновлении Airtable")
        xml = build_xml_response(
            mnt_id=MNT_ID,
            mnt_trx_id=mnt_trx_id_rx,
            result_code="500",
            description="Internal error",
            amount=mnt_amount_rx,
            attributes=None
        )
        return Response(content=xml, status_code=200, media_type="application/xml")

    # 8) Успешный ответ Moneta.ru
    xml = build_xml_response(
        mnt_id=MNT_ID,
        mnt_trx_id=mnt_trx_id_rx,
        result_code="200",
        description="Order paid successfully",
        amount=mnt_amount_rx,
        attributes=None
    )
    return Response(content=xml, status_code=200, media_type="application/xml")
