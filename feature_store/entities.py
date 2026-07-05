from feast import Entity

user = Entity(
    name="user_id",
    join_keys=["user_id"],
)

card = Entity(
    name="card_id",
    join_keys=["card_id"],
)
