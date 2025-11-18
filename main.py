from database import SessionLocal, init_db
from models import User


def seed_users(session):
    """Insert sample User records if table is empty."""
    if session.query(User).count() > 0:
        return

    samples = [
        User(name="Alice Nguyen", email="alice@example.com"),
        User(name="Binh Tran", email="binh@example.com"),
        User(name="Chi Le", email="chi@example.com"),
    ]
    session.add_all(samples)
    session.commit()


def main():
    # Initialize tables if not exist
    init_db()

    session = SessionLocal()
    try:
        seed_users(session)
        print("📌 Lấy toàn bộ user:")
        users = session.query(User).all()
        for u in users:
            print(f"ID: {u.id}, Name: {u.name}, Email: {u.email}")
    finally:
        session.close()


if __name__ == "__main__":
    main()