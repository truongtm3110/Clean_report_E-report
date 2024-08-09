from typing import Dict, Any

from pydantic import BaseModel


class CartInfo(BaseModel):
    id: int | None = None
    name: str | None = None
    price: float = 0.0
    cart_info_type: str | None = None
    duration: int | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "price": self.price,
            "duration": self.duration,
            "cart_info_type": self.cart_info_type
        }
