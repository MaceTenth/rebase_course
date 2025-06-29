import psycopg2
from datetime import datetime, timezone
import uuid
from db import get_connection

class UserRepository:
    @staticmethod
    def create_table():
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id VARCHAR(36) PRIMARY KEY NOT NULL,
                        email VARCHAR(200) NOT NULL UNIQUE,
                        full_name VARCHAR(200) NOT NULL,
                        joined_at TIMESTAMP NOT NULL,
                        deleted_since TIMESTAMP
                    )
                ''')
                conn.commit()

    @staticmethod
    def upsert_user(email: str, full_name: str):
        user_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        with get_connection() as conn:
            with conn.cursor() as cur:
                # First try to update existing user
                cur.execute('''
                    UPDATE users 
                    SET full_name = %s,
                        deleted_since = NULL
                    WHERE email = %s 
                      AND (full_name <> %s OR deleted_since IS NOT NULL)
                    RETURNING id, deleted_since
                ''', (full_name, email, full_name))
                
                result = cur.fetchone()
                
                if result:
                    # Update was successful
                    conn.commit()
                    return result, False  # False indicates no new record created
                
                # If no update occurred, insert new user
                cur.execute('''
                    INSERT INTO users (id, email, full_name, joined_at, deleted_since)
                    VALUES (%s, %s, %s, %s, NULL)
                    ON CONFLICT (email) DO NOTHING
                    RETURNING id, deleted_since
                ''', (user_id, email, full_name, now))
                
                result = cur.fetchone()
                conn.commit()
                return (result, True) if result else (None, False)  # True indicates new record created

    @staticmethod
    def get_user(email: str):
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    SELECT email, full_name, joined_at 
                    FROM users
                    WHERE email = %s AND deleted_since IS NULL
                ''', (email,))
                return cur.fetchone()

    @staticmethod
    def soft_delete_user(email: str):
        now = datetime.now(timezone.utc)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    UPDATE users SET deleted_since = %s
                    WHERE email = %s AND deleted_since IS NULL
                ''', (now, email))
                conn.commit()
                return cur.rowcount > 0