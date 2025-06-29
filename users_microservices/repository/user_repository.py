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
        now = datetime.now(timezone.utc)
        user_id = str(uuid.uuid4())
        with get_connection() as conn:
            with conn.cursor() as cur:
                # First check if user exists with same data
                cur.execute('''
                    SELECT id FROM users 
                    WHERE email = %s 
                    AND full_name = %s 
                    AND deleted_since IS NULL
                ''', (email, full_name))
                
                existing_user = cur.fetchone()
                if existing_user:
                    # User exists with exactly same data, no changes needed
                    return existing_user[0], None

                # Proceed with upsert if changes are needed
                cur.execute('''
                    WITH updated AS (
                        INSERT INTO users (id, email, full_name, joined_at, deleted_since)
                        VALUES (%s, %s, %s, %s, NULL)
                        ON CONFLICT (email) DO UPDATE 
                        SET full_name = CASE
                                WHEN users.deleted_since IS NOT NULL THEN %s
                                WHEN users.full_name <> %s THEN %s
                                ELSE users.full_name
                            END,
                            deleted_since = CASE
                                WHEN users.deleted_since IS NOT NULL THEN NULL
                                ELSE users.deleted_since
                            END
                        RETURNING id, 
                                CASE 
                                    WHEN xmax = 0 THEN true  -- This is a new insert
                                    ELSE false               -- This is an update
                                END as is_new
                    )
                    SELECT * FROM updated
                ''', (user_id, email, full_name, now, full_name, full_name, full_name))
                
                result = cur.fetchone()
                conn.commit()
                
                if not result:
                    return None, None
                    
                return result

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