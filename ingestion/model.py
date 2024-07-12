from pydantic import BaseModel

class JobParameters(BaseModel):
    file_path: str