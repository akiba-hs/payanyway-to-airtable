import os
import hashlib
import logging
import json
from typing import Dict, List, Tuple, Optional

from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
import httpx
import jwt
from xml.etree import ElementTree as ET
from xml.dom import minidom
from dotenv import load_dotenv

# ----------------------------------------------------------------------
# Загрузка переменных окружения (только для локальной разработки)
# .env должен содержать:
#   MNT_ID
#   MNT_INTEGRITY_CODE
#   AIRTABLE_API_KEY
#   AIRTABLE_BASE_ID
#   AIRTABLE_TABLE_NAME (опционально, по умолчанию "Payments")
# ----------------------------------------------------------------------
load_dotenv()

MNT_ID = os.getenv("MNT_ID", "").strip()
INTEGRITY_CODE = os.getenv("MNT_INTEGRITY_CODE", "").strip()
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Payments").strip()
AUTH_URL = os.getenv("AUTH_URL", "").strip()
PUBLIC_KEY = os.getenv("PUBLIC_KEY", "").replace("\\n", "\n").strip()

if not all([
    MNT_ID,
    INTEGRITY_CODE,
    AIRTABLE_API_KEY,
    AIRTABLE_BASE_ID,
    AUTH_URL,
    PUBLIC_KEY,
]):
    logging.critical(
        "Не установлены обязательные переменные окружения: "
        "MNT_ID, MNT_INTEGRITY_CODE, AIRTABLE_API_KEY, AIRTABLE_BASE_ID, "
        "AUTH_URL, PUBLIC_KEY"
    )

app = FastAPI()


def calculate_signature(params: Dict[str, str]) -> str:
    """
    Вычисляем MD5-подпись входящего запроса Moneta.ru:
    MD5(MNT_ID + MNT_TRANSACTION_ID + MNT_OPERATION_ID +
        MNT_AMOUNT + MNT_CURRENCY_CODE + MNT_SUBSCRIBER_ID +
        MNT_TEST_MODE + INTEGRITY_CODE)

    Считаем, что params["MNT_AMOUNT"] уже передано как строка (с двумя знаками после точки).
    """
    mnt_trx_id     = params.get("MNT_TRANSACTION_ID", "")
    mnt_op_id      = params.get("MNT_OPERATION_ID", "")
    mnt_amount_str = params.get("MNT_AMOUNT", "")          # берем "как есть"
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


def calc_payment_url(payment_id: str, amount: str, description: str, base_url: str) -> str:
    """Формирует ссылку на оплату PayAnyWay."""
    options = {
        "MNT_ID": MNT_ID,
        "MNT_TRANSACTION_ID": payment_id,
        "MNT_AMOUNT": amount,
        "MNT_DESCRIPTION": description,
        "MNT_CURRENCY_CODE": "RUB",
        "MNT_TEST_MODE": "0",
        "MNT_SUCCESS_URL": base_url,
        "MNT_FAIL_URL": base_url,
    }
    # httpx.QueryParams does not provide a ``render`` method. ``str()`` will
    # properly encode the parameters into a query string.
    query_string = str(httpx.QueryParams(options))
    return "https://www.payanyway.ru/assistant.htm?" + query_string


async def update_airtable_record(record_id: str, amount: str, status: str) -> None:
    """
    Обновление записи в Airtable:
    PATCH https://api.airtable.com/v0/{BASE_ID}/{TABLE}/{record_id}

    Поля:
      - Amount (строка, уже правильно отформатирована)
      - Status ("Paid" или "Test Paid")
    """
    airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}/{record_id}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }
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
            "Ошибка при обновлении Airtable (record_id=%s): %s %s",
            record_id, resp.status_code, resp.text
        )
        raise HTTPException(status_code=500, detail="Airtable update failed")

async def get_airtable_email(record_id: str) -> str:
    """
    Асинхронно получает запись из таблицы Payments (Airtable) по record_id
    и возвращает значение поля 'Email'. Если поле не найдено или запрос провалился,
    выбрасывает HTTPException(404) или HTTPException(500).
    """
    # Составляем URL: https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_id}
    airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}/{record_id}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }

    # Делаем GET-запрос к Airtable
    async with httpx.AsyncClient() as client:
        response = await client.get(airtable_url, headers=headers, timeout=10.0)

    # Если статус ответа не 200, выбрасываем ошибку
    if response.status_code == 404:
        # Запись не найдена
        raise HTTPException(status_code=404, detail=f"Record {record_id} not found in Airtable")
    if response.status_code != 200:
        # Другая ошибка на стороне Airtable
        raise HTTPException(
            status_code=500,
            detail=f"Airtable GET failed: {response.status_code} {response.text}"
        )

    # Парсим JSON-ответ
    try:
        data = response.json()
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Invalid JSON from Airtable")

    # Проходим по ключу "fields" и ищем "Email"
    fields = data.get("fields", {})
    email = fields.get("Email (from Resident)")
    if not email:
        # Поле Email отсутствует или пустое
        raise HTTPException(status_code=404, detail=f"Email not found in record {record_id}")

    return email[0]


async def find_invoices(username: str, user_id: str) -> List[Dict[str, object]]:
    """Возвращает записи из Airtable для пользователя."""
    base_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient() as client:
        formula = f"OR(FIND('@{username}', ARRAYJOIN(ARRAYUNIQUE({{Telegram Username (from Resident)}}))), FIND('{user_id}', ARRAYJOIN(ARRAYUNIQUE({{Telegram Username (from Resident)}}))))"
        resp = await client.get(base_url, headers=headers, params={"filterByFormula": formula})
        if resp.status_code != 200:
            logging.error("Airtable search failed: %s %s", resp.status_code, resp.text)
            return []
        data = resp.json().get("records", [])
    return data


def build_xml_response(
    mnt_id: str,
    mnt_trx_id: str,
    result_code: str,
    attributes: Optional[Dict[str, str]] = None
) -> str:
    """
    Формирует XML-ответ Moneta.ru по новой спецификации:

    <?xml version="1.0" encoding="UTF-8" ?>
    <MNT_RESPONSE>
      <MNT_ID>...</MNT_ID>
      <MNT_TRANSACTION_ID>...</MNT_TRANSACTION_ID>
      <MNT_RESULT_CODE>...</MNT_RESULT_CODE>
      <MNT_SIGNATURE>...</MNT_SIGNATURE>
      <MNT_ATTRIBUTES>
        <ATTRIBUTE>
          <KEY>...</KEY>
          <VALUE>...</VALUE>
        </ATTRIBUTE>
        ...
      </MNT_ATTRIBUTES>
    </MNT_RESPONSE>

    Подпись считается как MD5(MNT_RESULT_CODE + MNT_ID + MNT_TRANSACTION_ID + INTEGRITY_CODE).
    Значения атрибутов в <VALUE> не должны содержать кавычек, символов &, $, #, /, \.
    """
    # 1. Создаём корень <MNT_RESPONSE>
    root = ET.Element("MNT_RESPONSE")

    # 2. Добавляем MNT_ID и MNT_TRANSACTION_ID
    ET.SubElement(root, "MNT_ID").text = mnt_id
    ET.SubElement(root, "MNT_TRANSACTION_ID").text = mnt_trx_id

    # 3. Добавляем MNT_RESULT_CODE
    ET.SubElement(root, "MNT_RESULT_CODE").text = result_code

    # 4. Вычисляем подпись
    sign_src = result_code + mnt_id + mnt_trx_id + INTEGRITY_CODE
    mnt_sig = hashlib.md5(sign_src.encode("utf-8")).hexdigest()
    ET.SubElement(root, "MNT_SIGNATURE").text = mnt_sig

    # 5. Формируем блок MNT_ATTRIBUTES
    attrs_elem = ET.SubElement(root, "MNT_ATTRIBUTES")
    if attributes:
        # Для каждого ключа и строки-значения создаём <ATTRIBUTE>
        for key, value_str in attributes.items():
            attr = ET.SubElement(attrs_elem, "ATTRIBUTE")
            ET.SubElement(attr, "KEY").text = key
            ET.SubElement(attr, "VALUE").text = value_str
    # Если attributes=None или пустой dict — <MNT_ATTRIBUTES/> будет пустым

    # 6. Преобразуем ElementTree в разбитый по отступам XML
    rough_xml = ET.tostring(root, encoding="utf-8")
    parsed = minidom.parseString(rough_xml)
    pretty_bytes = parsed.toxml(encoding="utf-8")
    return pretty_bytes.decode("utf-8")


@app.get("/", response_class=HTMLResponse)
async def invoices(request: Request) -> Response:
    token = request.cookies.get("token")
    if not token:
        return RedirectResponse(
            url=f"{AUTH_URL}?redirect_uri={request.url}&error=Unauthorized",
            status_code=302,
        )
    try:
        payload = jwt.decode(token, PUBLIC_KEY, algorithms=["RS256"])
    except Exception as e:
        return RedirectResponse(
            url=f"{AUTH_URL}?redirect_uri={request.url}&error={e!r}",
            status_code=302,
        )

    username = payload.get("username")
    user_id = payload.get("id")

    records = await find_invoices(username or "", str(user_id or ""))
    rows = []
    for rec in records:
        f = rec.get("fields", {})
        amount = f.get("Amount")
        method = f.get("Method")
        month = f.get("Month")

        resident_val = f.get("Resident")
        if isinstance(resident_val, list):
            resident = ", ".join(resident_val)
        else:
            resident = resident_val

        # Если поле содержит id записи Airtable, пробуем взять имя из lookup
        if resident and isinstance(resident, str) and resident.startswith("rec"):
            name_field = f.get("Name (from Resident)")
            if name_field:
                resident = ", ".join(name_field) if isinstance(name_field, list) else name_field

        status = f.get("Status")
        pay_link = ""
        if status == "Unpaid" and f.get("Method") == "Auto Credit Card":
            description = f"Резидентство за {month} ({resident})"
            pay_link = calc_payment_url(
                str(f.get("Payment Id")), f"{float(amount):.2f}", description, request.base_url
            )

        link_html = (
            f'<a class="pay-link" href="{pay_link}">Оплатить</a>'
            if pay_link
            else '<span class="no-pay">Не оплачивается</span>'
        )

        row_class = ""
        if status == "Paid":
            row_class = "paid-row"
        elif status == "Unpaid":
            row_class = "unpaid-row"
        elif status == "Test Paid":
            row_class = "test-paid-row"

        rows.append(
            f"<tr class='{row_class}'><td>{resident}</td><td>{month}</td>"
            f"<td>{method}</td><td>{amount}</td><td>{status}</td><td>{link_html}</td></tr>"
        )

    table_rows = "".join(rows)
    html_content = f"""
    <!DOCTYPE html>
    <html lang='en'>
    <head>
        <meta charset='utf-8'>
        <meta name='viewport' content='width=device-width, initial-scale=1'>
        <title>Invoices</title>
        <style>
            body {{ font-family: Arial, sans-serif; padding: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            .table-container {{ overflow-x: auto; }}
            th, td {{ border: 1px solid #ccc; padding: 8px 12px; text-align: left; }}
            th {{ background-color: #f5f5f5; }}
            a.pay-link {{ color: white; background: #007bff; padding: 4px 8px; border-radius: 4px; text-decoration: none; }}
            .no-pay {{ color: #777; font-style: italic; }}
            .paid-row {{ background-color: #e6ffed; }}
            .unpaid-row {{ background-color: #ffecec; }}
            .test-paid-row {{ background-color: #fff9e6; }}
            @media (max-width: 600px) {{
                body {{ padding: 10px; }}
                th, td {{ padding: 6px 8px; }}
                a.pay-link {{ display: inline-block; margin-top: 4px; }}
            }}
        </style>
    </head>
    <body>
        <h1>Ваши инвойсы</h1>
        <div class="table-container">
        <table>
            <tr>
                <th>Резидент</th><th>Месяц</th><th>Способ</th><th>Сумма</th><th>Статус</th><th></th>
            </tr>
            {table_rows}
        </table>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.api_route("/webhook", methods=["GET", "POST"])
async def moneta_webhook(request: Request) -> Response:
    """
    Эндпоинт /webhook:
    1) Если ни один параметр не передан (ни в query, ни в form-data, ни в JSON) -
       возвращаем 200 OK (health check) с text/plain.
    2) Иначе:
       a) Собираем параметры в словарь.
       b) Проверяем MNT_ID и MNT_SIGNATURE.
       c) Обновляем запись в Airtable.
       d) Формируем XML-ответ по новой схеме и возвращаем с Content-Type: application/xml.
    """
    # 1. Health check: нет ни query_params, ни form-data, ни JSON
    has_query = len(request.query_params) > 0
    has_form = False
    has_json = False

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
        # Определили как health check
        return Response(status_code=200, content="OK", media_type="text/plain")

    # 2. Сбор всех параметров в единый словарь строковых значений
    params: Dict[str, str] = {}
    # 2.1. Query-параметры
    for k, v in request.query_params.items():
        params[k] = v
    # 2.2. Form-data (если есть)
    if content_type.startswith("application/x-www-form-urlencoded") and has_form:
        for k, v in form_data.items():
            params[k] = v
    # 2.3. JSON (если есть)
    if content_type.startswith("application/json") and has_json:
        for k, v in json_body.items():
            params[k] = str(v)

    # 3. Извлекаем обязательные поля
    mnt_id_rx        = params.get("MNT_ID", "")
    mnt_trx_id_rx    = params.get("MNT_TRANSACTION_ID", "")
    mnt_signature_rx = params.get("MNT_SIGNATURE", "").lower()
    mnt_amount_rx    = params.get("MNT_AMOUNT", "")
    mnt_test_mode_rx = params.get("MNT_TEST_MODE", "0")

    # 4. Проверка MNT_ID
    if mnt_id_rx != MNT_ID:
        logging.error("MNT_ID mismatch")
        xml_fail = build_xml_response(
            mnt_id=mnt_id_rx or "",
            mnt_trx_id=mnt_trx_id_rx or "",
            result_code="500",
            attributes=None
        )
        return Response(content=xml_fail, status_code=200, media_type="application/xml")

    # 5. Проверка подписи входящего запроса
    calc_sig = calculate_signature(params)
    if calc_sig != mnt_signature_rx:
        logging.error(f"Signature mismatch: {calc_sig} expected, got {mnt_signature_rx}")
        xml_fail = build_xml_response(
            mnt_id=MNT_ID,
            mnt_trx_id=mnt_trx_id_rx or "",
            result_code="500",
            attributes=None
        )
        return Response(content=xml_fail, status_code=200, media_type="application/xml")

    email = await get_airtable_email(mnt_trx_id_rx)
    
    # 6. Обновляем Airtable
    status_value = "Test Paid" if mnt_test_mode_rx == "1" else "Paid"
    try:
        # Передаём сумму как строку (она уже в нужном формате)
        await update_airtable_record(
            record_id=mnt_trx_id_rx,
            amount=mnt_amount_rx,
            status=status_value
        )
    except HTTPException as e:
        logging.exception("Airtable update HTTPException")
        xml_fail = build_xml_response(
            mnt_id=MNT_ID,
            mnt_trx_id=mnt_trx_id_rx,
            result_code="500",
            attributes=None
        )
        return Response(content=xml_fail, status_code=200, media_type="application/xml")
    except Exception:
        logging.exception("Airtable update Exception")
        xml_fail = build_xml_response(
            mnt_id=MNT_ID,
            mnt_trx_id=mnt_trx_id_rx,
            result_code="500",
            attributes=None
        )
        return Response(content=xml_fail, status_code=200, media_type="application/xml")

    # 7. Подготавливаем атрибуты для XML-ответа
    attributes: Dict[str, str] = {
        # INVENTORY и CLIENT должны быть валидным JSON-массивом в виде строки
        "INVENTORY": json.dumps([{
            "name": "Подписка на мероприятия",
            "price": float(mnt_amount_rx),
            "quantity": 1,
            "vatTag": "1105",
            "pm": "full_payment",
            "po": "commodity"
        }]), 
        # CUSTOMER — email покупателя
        "CUSTOMER": email,  
    }
    # Если какое-то значение пустое, Moneta всё равно пропустит, но можно убрать пустые ключи:
    attributes = {k: v for k, v in attributes.items() if v}

    # 8. Успешный XML-ответ
    xml_success = build_xml_response(
        mnt_id=MNT_ID,
        mnt_trx_id=mnt_trx_id_rx,
        result_code="200",
        attributes=attributes
    )
    return Response(content=xml_success, status_code=200, media_type="application/xml")
