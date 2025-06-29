from pydantic import BaseModel, field_validator
import re

class UserIn(BaseModel):
    email: str
    full_name: str

    @field_validator('email')
    @classmethod
    def validate_email(cls, v):
        if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", v):
            raise ValueError('Invalid email format')
        return v

class UserOut(BaseModel):
    email: str
    full_name: str
    joined_at: str