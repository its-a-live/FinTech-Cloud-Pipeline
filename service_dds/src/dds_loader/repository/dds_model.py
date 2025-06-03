from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime
from decimal import Decimal


class UserObj(BaseModel):
    h_user_pk: UUID
    user_id: str
    user_name: str
    user_login: str


class ProductObj(BaseModel):
    h_product_pk: UUID
    product_id: str
    product_name: str
    hk_product_names_hashdiff: UUID
    category_name: str


class CategoryObj(BaseModel):
    h_category_pk: UUID
    category_name: str


class RestaurantObj(BaseModel):
    h_restaurant_pk: UUID
    restaurant_id: str
    restaurant_name: str
    hk_restaurant_names_hashdiff: UUID


class OrderObj(BaseModel):
    h_order_pk: UUID
    order_id: int
    order_dt: datetime
    cost: Decimal = Field(max_digits=19, decimal_places=5)
    payment: Decimal = Field(max_digits=19, decimal_places=5)
    hk_order_cost_hashdiff: UUID
    status: str
    hk_order_status_hashdiff: UUID


class OutputMsgObj(BaseModel):
    user_id: str
    product_id: str
    product_name: str
    order_cnt: int
