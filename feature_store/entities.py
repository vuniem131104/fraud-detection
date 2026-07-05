from feast import Entity, ValueType

user = Entity(name="user_id", join_keys=["user_id"], value_type=ValueType.INT64)
card = Entity(name="card_id", join_keys=["card_id"], value_type=ValueType.STRING)