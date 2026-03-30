import random
from typing import Any


def generate_users(count: int, seed: int = 42) -> list[dict[str, Any]]:
    """Genera un dataset determinístico de usuarios."""
    rng = random.Random(seed)
    
    cities = ["New York", "London", "Tokyo", "Madrid", "Berlin", "Paris"]
    roles = ["admin", "user", "guest", "editor"]
    
    docs = []
    for i in range(count):
        docs.append({
            "_id": i,
            "username": f"user_{i}_{rng.randint(1000, 9999)}",
            "age": rng.randint(18, 80),
            "city": rng.choice(cities),
            "role": rng.choice(roles),
            "score": rng.random() * 100.0,
            "active": rng.random() > 0.2,
            "tags": [rng.choice(["tech", "sports", "music", "art"]) for _ in range(rng.randint(0, 5))]
        })
    return docs

def generate_orders(count: int, user_count: int, seed: int = 43) -> list[dict[str, Any]]:
    """Genera un dataset de pedidos vinculados a usuarios."""
    rng = random.Random(seed)
    
    docs = []
    for i in range(count):
        docs.append({
            "_id": f"order_{i}",
            "user_id": rng.randint(0, user_count - 1),
            "amount": round(rng.random() * 500.0, 2),
            "status": rng.choice(["pending", "shipped", "delivered", "cancelled"]),
            "items": rng.randint(1, 10)
        })
    return docs
