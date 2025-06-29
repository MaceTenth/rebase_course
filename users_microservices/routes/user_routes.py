from fastapi import APIRouter, HTTPException, Response, status
from models.user import UserIn, UserOut
from repository.user_repository import UserRepository
from logging_config import logger, structured_log
from datetime import timezone

router = APIRouter()

@router.post("/users/")
def upsert_user(user: UserIn, response: Response):
    result = UserRepository.upsert_user(user.email, user.full_name)
    if not result:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"detail": "Operation failed"}
        
    user_id, is_new = result
    
    if is_new is None:
        # No changes were needed (user exists with same data)
        response.status_code = status.HTTP_200_OK
        return {"detail": "No changes required"}
    elif is_new:
        # New user created
        logger.info(structured_log(
            "User created",
            event="user_created",
            user_id=user_id,
            email=user.email,
            operation="upsert"
        ))
        response.status_code = status.HTTP_201_CREATED
        return {"detail": "User created"}
    else:
        # Existing user updated
        logger.info(structured_log(
            "User updated",
            event="user_updated",
            user_id=user_id,
            email=user.email,
            operation="upsert"
        ))
        response.status_code = status.HTTP_200_OK
        return {"detail": "User updated"}

@router.get("/users/{email}", response_model=UserOut)
def get_user(email: str):
    user = UserRepository.get_user(email)
    if not user:
        logger.info(structured_log(
            "User not found",
            event="user_not_found",
            email=email,
            operation="get"
        ))
        raise HTTPException(status_code=404, detail="User not found")
    
    logger.info(structured_log(
        "User retrieved",
        event="user_retrieved",
        email=email,
        operation="get"
    ))
    return UserOut(
        email=user[0],
        full_name=user[1],
        joined_at=user[2].replace(tzinfo=timezone.utc).isoformat()
    )

@router.delete("/users/{email}", status_code=204)
def delete_user(email: str):
    was_deleted = UserRepository.soft_delete_user(email)
    if was_deleted:
        logger.info(structured_log(
            "User soft deleted",
            event="user_soft_deleted",
            email=email,
            operation="delete"
        ))
    else:
        logger.info(structured_log(
            "User not found or inactive for deletion",
            event="user_not_found_or_inactive",
            email=email,
            operation="delete"
        ))
    return