from datetime import datetime, timezone
from decimal import Decimal
import re

from pydantic import AwareDatetime, BaseModel, field_serializer, field_validator


class ContractModel(BaseModel):
    @field_validator("*", mode="before", check_fields=False)
    @classmethod
    def validate_input(cls, value, info):
        if isinstance(value, str):
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                raise ValueError("Некорректный Unicode")
        annotation = cls.model_fields[info.field_name].annotation
        if annotation is Decimal:
            if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
                raise ValueError("Ожидается JSON number")
        if annotation is AwareDatetime and not isinstance(value, datetime):
            if not isinstance(value, str) or not re.fullmatch(
                r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:[Zz]|[+-][0-9]{2}:[0-9]{2})", value,
            ):
                raise ValueError("Ожидается строка date-time с часовым поясом")
        return value

    @field_validator("*", check_fields=False)
    @classmethod
    def validate_datetime(cls, value):
        if isinstance(value, datetime):
            try:
                return value.astimezone(timezone.utc)
            except OverflowError:
                raise ValueError("Дата выходит за диапазон 0001–9999 после перевода в UTC")
        return value

    @field_serializer("*", mode="wrap", when_used="json", check_fields=False)
    def serialize_decimal(self, value, handler):
        return float(value) if isinstance(value, Decimal) else handler(value)
