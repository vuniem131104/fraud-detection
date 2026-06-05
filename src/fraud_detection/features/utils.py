def build_transactions_key(user_id: str, card_id: str) -> str:
        return f"user:card:transactions:{user_id}_{card_id}"

def build_features_key(user_id: str, card_id: str) -> str:
    return f"user:card:features:{user_id}_{card_id}"
