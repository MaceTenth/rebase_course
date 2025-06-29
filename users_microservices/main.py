# main.py
from fastapi import FastAPI
from routes.user_routes import router
from repository.user_repository import UserRepository

app = FastAPI()

# Initialize the database
UserRepository.create_table()

# Include the router
app.include_router(router)
