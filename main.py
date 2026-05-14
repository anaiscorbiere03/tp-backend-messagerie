#on initialise: Partie 1
from sqlmodel import SQLModel, Field, Session, create_engine, select
from sqlalchemy import text
from typing import List, Optional
from datetime import datetime as dt_datetime
from pydantic import EmailStr
from fastapi import FastAPI, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
import datetime as dt

# Database models
class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(unique=True)
    email: EmailStr = Field(unique=True)

class Message(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    sender_id: int = Field(foreign_key="user.id")
    receiver_id: int = Field(foreign_key="user.id")
    subject: str
    body: str
    is_read: bool = Field(default=False)
    created_at: dt_datetime = Field(default_factory=dt_datetime.utcnow)

# Pydantic schemas
class UserCreate(SQLModel):
    username: str
    email: EmailStr

class UserRead(SQLModel):
    id: int
    username: str
    email: str

class MessageCreate(SQLModel):
    sender_id: int
    receiver_id: int
    subject: str
    body: str

class MessageRead(SQLModel):
    id: int
    sender_id: int
    receiver_id: int
    subject: str
    body: str
    is_read: bool
    created_at: dt_datetime

# Database setup
database_url = "sqlite:///./messagerie.db"
engine = create_engine(database_url, echo=True)
SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session

app = FastAPI()

# WebSocket Connection Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.username_table: dict[WebSocket, str] = {}
        self.inbox_history: dict[str, list[dict]] = {}
        self.sent_history: dict[str, list[dict]] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        if websocket in self.username_table:
            del self.username_table[websocket]

    async def broadcast(self, data: dict):
        # Broadcast the dictionary as a JSON object
        for connection in self.active_connections:
            await connection.send_json(data)

    async def broadcast_userlist(self):
        await self.broadcast({"action": "userlist", "users": self.get_current_users()})

    async def send_to_user(self, username: str, data: dict):
        for ws, uname in self.username_table.items():
            if uname == username:
                await ws.send_json(data)
                return True
        return False

    def get_current_users(self):
        return list(self.username_table.values())

manager = ConnectionManager()

# User routes
@app.post("/users", response_model=UserRead)
def create_user(user: UserCreate, session: Session = Depends(get_session)):
    # Check if username or email exists
    existing = session.exec(select(User).where((User.username == user.username) | (User.email == user.email))).first()
    if existing:
        raise HTTPException(status_code=400, detail="Username or email already exists")
    db_user = User.from_orm(user)
    session.add(db_user)
    session.commit()
    session.refresh(db_user)
    return db_user

@app.get("/users", response_model=List[UserRead])
def list_users(session: Session = Depends(get_session)):
    users = session.exec(select(User)).all()
    return users

@app.get("/users/{user_id}", response_model=UserRead)
def get_user(user_id: int, session: Session = Depends(get_session)):
    user = session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

# Message routes
@app.post("/messages", response_model=MessageRead)
def send_message(message: MessageCreate, session: Session = Depends(get_session)):
    # Check sender and receiver exist
    sender = session.get(User, message.sender_id)
    receiver = session.get(User, message.receiver_id)
    if not sender or not receiver:
        raise HTTPException(status_code=404, detail="Sender or receiver not found")
    if message.sender_id == message.receiver_id:
        raise HTTPException(status_code=400, detail="Cannot send message to yourself")
    if not message.subject.strip() or not message.body.strip():
        raise HTTPException(status_code=400, detail="Subject and body cannot be empty")
    db_message = Message.from_orm(message)
    session.add(db_message)
    session.commit()
    session.refresh(db_message)
    return db_message

@app.get("/users/{user_id}/inbox", response_model=List[MessageRead])
def get_inbox(user_id: int, unread_only: bool = Query(False), limit: int = Query(50), offset: int = Query(0), search: str = Query(None), session: Session = Depends(get_session)):
    query = select(Message).where(Message.receiver_id == user_id)
    if unread_only:
        query = query.where(text("is_read = 0"))
    if search:
        # Use text with bindparam to avoid SQL injection
        query = query.where(text("subject LIKE '%' || :search || '%'")).bindparams(search=search)
    query = query.order_by(text("created_at DESC")).offset(offset).limit(limit)
    messages = session.exec(query).all()
    return messages

@app.get("/users/{user_id}/sent", response_model=List[MessageRead])
def get_sent(user_id: int, limit: int = Query(50), offset: int = Query(0), session: Session = Depends(get_session)):
    messages = session.exec(
        select(Message).where(Message.sender_id == user_id).order_by(text("created_at DESC")).offset(offset).limit(limit)
    ).all()
    return messages

@app.get("/messages/{message_id}", response_model=MessageRead)
def get_message(message_id: int, session: Session = Depends(get_session)):
    message = session.get(Message, message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    return message

@app.patch("/messages/{message_id}/read")
def mark_as_read(message_id: int, session: Session = Depends(get_session)):
    message = session.get(Message, message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    message.is_read = True
    session.commit()
    return {"detail": "Message marked as read"}

@app.get("/users/{user_id}/unread_count")
def get_unread_count(user_id: int, session: Session = Depends(get_session)):
    count = session.exec(
        select(Message).where((Message.receiver_id == user_id) & (not Message.is_read))
    ).all()
    return {"unread_count": len(count)}

# WebSocket endpoint
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    print(f"WebSocket connection attempt from {client_id}")
    try:
        await manager.connect(websocket)
        print(f"WebSocket accepted for {client_id}")
        # Send current user list to the new connection
        current_users = manager.get_current_users()
        await websocket.send_json({"action": "userlist", "users": current_users})
    except Exception as e:
        print(f"Error connecting {client_id}: {e}")
        await websocket.close(code=1000)
        return
    try:
        while True:
            # Receive JSON from a client
            data = await websocket.receive_json()
            print(f"received data: {data}")

            action = data["action"]
            if action == "setusername":
                userName = data["data"]
                if userName in manager.get_current_users():
                    await websocket.send_json({"action": "error", "message": "Username already taken"})
                else:
                    manager.username_table[websocket] = userName
                    manager.inbox_history.setdefault(userName, [])
                    manager.sent_history.setdefault(userName, [])
                    print(f"Register user: {userName}")
                    await manager.broadcast({"action": "newuser", "status": "on", "userName": userName, "message": f"New user {userName}"})
                    await manager.broadcast_userlist()
                    await websocket.send_json({
                        "action": "history",
                        "inbox": manager.inbox_history[userName],
                        "sent": manager.sent_history[userName]
                    })
            elif action == "sendmessage":
                message = data["message"]
                recipient = data["recipient"]
                subject = data.get("subject", "No subject")
                sender = manager.username_table.get(websocket, client_id)
                if sender == recipient:
                    await websocket.send_json({"action": "error", "message": "Cannot send message to yourself"})
                    continue
                if not message.strip() or not subject.strip():
                    await websocket.send_json({"action": "error", "message": "Subject and message cannot be empty"})
                    continue
                payload = {
                    "action": "message",
                    "sender": sender,
                    "subject": subject,
                    "message": message,
                    "time": dt.datetime.now().strftime("%H:%M:%S")
                }
                sent = await manager.send_to_user(recipient, payload)
                if sent:
                    manager.sent_history.setdefault(sender, []).append({
                        "subject": subject,
                        "recipient": recipient,
                        "message": message,
                        "time": payload["time"]
                    })
                    manager.inbox_history.setdefault(recipient, []).append({
                        "subject": subject,
                        "sender": sender,
                        "message": message,
                        "time": payload["time"],
                        "read": False
                    })
                else:
                    await websocket.send_json({"action": "error", "message": "User does not exist"})
            else:
                # Enrich the data with server-side info (timestamp)
                sender = manager.username_table.get(websocket, client_id)
                payload = {
                    "status": data["status"],
                    "sender": sender,
                    "time": dt.datetime.now().strftime("%H:%M:%S")
                }
                await manager.broadcast(payload)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast_userlist()
