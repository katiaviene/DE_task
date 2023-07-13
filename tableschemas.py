import pydantic as pyd


class BrandSchema(pyd.BaseModel):
    brand_id: int = pyd.Field(ge=1)
    brand_name: str


class CategoriesSchema(pyd.BaseModel):
    category_id: int = pyd.Field(ge=1)
    category_name = str
