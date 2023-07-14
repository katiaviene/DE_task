import pydantic as pyd
from typing import Optional
from datetime import date


class BrandSchema(pyd.BaseModel):
    brand_id: int = pyd.Field(ge=1)
    brand_name: str


class CategoriesSchema(pyd.BaseModel):
    category_id: int = pyd.Field(ge=1)
    category_name: str


class CustomerSchema(pyd.BaseModel):
    customer_id: int = pyd.Field(ge=1)
    first_name: str
    last_name: str
    phone: Optional[str] = pyd.Field(None, max_length=14)
    email: pyd.EmailStr
    street: str
    city: str
    state: str = pyd.Field(max_length=2)
    zip_code: str


class ProductSchema(pyd.BaseModel):
    product_id: int = pyd.Field(ge=1)
    product_name: str
    brand_id: int
    category_id: int
    model_year: int
    list_price: pyd.condecimal(decimal_places=2)

    @pyd.validator('model_year')
    def validate_year(cls, value):
        if value < 1900 or value > 2100:
            raise ValueError('Year must be between 1900 and 2100')
        return value

class OrderItemSchema(pyd.BaseModel):
    order_id: int = pyd.Field(ge=1)
    item_id: int
    product_id: int
    quantity: int
    list_price: pyd.condecimal(decimal_places=2)
    discount: pyd.condecimal(decimal_places=2)

class OrdersSchema(pyd.BaseModel):
    order_id: int = pyd.Field(ge=1)
    customer_id: int
    order_status: int
    order_date: date
    required_date: Optional[date]
    shipped_date: Optional[date]
    store_id: int
    staff_id: int

    @pyd.validator('required_date')
    def validate_required(cls, value, values):
        if 'required_date' in values and value <= values['order_date']:
            raise ValueError('required_date must be greater than order_date')
        return value

    @pyd.validator('shipped_date')
    def validate_shipped(cls, value, values):
        if 'shipped_date' in values and value <= values['order_date']:
            raise ValueError('required_date must be greater than order_date')
        return value